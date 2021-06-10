package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

const (
	LetterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	MockFileSize = 100
)

type MockDownloader struct{}

func (d *MockDownloader) GetDownloadSize(url string) (fileSize int64, err error) {
	return MockFileSize, nil
}

func (d *MockDownloader) DownloadFile(ctx context.Context, url, filepath string, updater ProgressUpdater) (written int64, err error) {
	f, err := os.Create(filepath)
	if err != nil {
		return 0, err
	}
	f.Close()
	if err := os.Truncate(filepath, MockFileSize); err != nil {
		return 0, err
	}

	for i := 1; i <= MockFileSize; i++ {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("cancelled mock processing")
		default:
			updater.UpdateProgress(1)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return MockFileSize, nil
}

func BenchmarkDownload(b *testing.B) {
	logrus.SetLevel(logrus.DebugLevel)
	address := fmt.Sprintf("localhost:%d", types.DefaultDataSourceServerPort)
	fileName := "data-source-download-file"

	dir, err := prepareTestWorkDirectory()
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := generateDiskConfigFile(dir); err != nil {
		b.Fatal(err)
	}

	go NewServer(address, fileName, string(types.DataSourceTypeDownload), map[string]string{types.DataSourceTypeDownloadParameterURL: "http://mock-download"}, dir, &MockDownloader{})
	time.Sleep(time.Second)

	cli := client.DataSourceClient{
		Remote: address,
	}

	isDownloaded := false
	for count := 0; count < 30; count++ {
		dsInfo, err := cli.Get()
		if err != nil {
			b.Fatal(err)
		}
		if dsInfo.State == string(types.StateReady) && dsInfo.Progress == 100 {
			isDownloaded = true
			break
		}
		time.Sleep(time.Second)
	}
	if !isDownloaded {
		return
	}

	downloadedFilePath := filepath.Join(dir, types.DataSourceDirectoryName, fileName)
	stat, err := os.Stat(downloadedFilePath)
	if err != nil {
		b.Fatal(err)
	}
	if stat.Size() != MockFileSize {
		b.Fatalf("The test download file size %v is different from the expected size %v", stat.Size(), MockFileSize)
	}
}

func BenchmarkUpload(b *testing.B) {
	logrus.SetLevel(logrus.DebugLevel)

	address := fmt.Sprintf("localhost:%d", types.DefaultDataSourceServerPort)
	fileName := "data-source-upload-file"
	originalFileName := "data-source-original-file"

	dir, err := prepareTestWorkDirectory()
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := generateDiskConfigFile(dir); err != nil {
		b.Fatal(err)
	}

	originalFilePath := filepath.Join(dir, originalFileName)
	err = GenerateRandomDataFile(originalFilePath, 500)
	if err != nil {
		b.Fatal(err)
	}

	go NewServer(address, fileName, string(types.DataSourceTypeUpload), map[string]string{}, dir, &MockDownloader{})
	time.Sleep(time.Second)

	b.ResetTimer()
	cli := client.DataSourceClient{
		Remote: address,
	}

	go func() {
		if err := cli.Upload(originalFilePath); err != nil {
			b.Fatal(err)
		}
	}()

	isUploaded := false
	for count := 0; count < 30; count++ {
		dsInfo, err := cli.Get()
		if err != nil {
			b.Fatal(err)
		}
		if dsInfo.State == string(types.StateReady) && dsInfo.Progress == 100 {
			isUploaded = true
			break
		}
		logrus.Debugf("Current upload progress: %v%%(%vByte)", dsInfo.Progress, dsInfo.ProcessedSize)
		time.Sleep(time.Second)
	}
	if !isUploaded {
		dsInfo, err := cli.Get()
		if err != nil {
			b.Fatalf("Failed to get info after upload failure: %v", err)
		}
		b.Fatalf("Failed to finish upload within 30 second, current state: %v, message: %v", dsInfo.State, dsInfo.Message)
	}

	uploadedFilePath := filepath.Join(dir, types.DataSourceDirectoryName, fileName)
	err = exec.Command("diff", originalFilePath, uploadedFilePath).Run()
	if err != nil {
		b.Fatal(err)
	}
}

func prepareTestWorkDirectory() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(currentUser.HomeDir, "data-source-test-dir")
	if err = os.RemoveAll(dir); err != nil {
		return "", err
	}
	err = os.Mkdir(dir, 0777)
	if err != nil {
		return "", err
	}
	return dir, nil
}

func generateDiskConfigFile(dir string) error {
	diskCfgPath := filepath.Join(dir, util.DiskConfigFile)
	if _, err := os.Stat(diskCfgPath); os.IsNotExist(err) {
		diskCfg := &util.DiskConfig{
			DiskUUID: "bim-test-disk-cfg",
		}
		encodedDiskCfg, err := json.Marshal(diskCfg)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(diskCfgPath, encodedDiskCfg, 0777)
		if err != nil {
			return err
		}
	} else {
		if err != nil {
			return err
		}
	}
	return nil
}

func RandStringBytes(n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = LetterBytes[rand.Intn(len(LetterBytes))]
	}
	return b
}

func GenerateRandomDataFile(filePath string, sizeInMB int) error {
	// 1Mi
	chunkSize := int64(1 * 1024 * 1024)

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	defer f.Sync()
	if err := f.Truncate(int64(sizeInMB) * chunkSize); err != nil {
		return err
	}

	data := make([]byte, chunkSize)
	for i := 0; i < sizeInMB; i++ {
		data = RandStringBytes(chunkSize)
		if _, err := f.WriteAt(data, int64(i)*chunkSize); err != nil {
			return err
		}
	}

	return nil
}
