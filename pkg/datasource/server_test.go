package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/sync"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"

	. "gopkg.in/check.v1"
)

const (
	MockFileSize = 4096

	TestBackingImageUUID = "test-backing-image-uuid"

	TestDataSourceServerPort = 8100
	TestSyncServerPort       = 8101
)

func Test(t *testing.T) { TestingT(t) }

type DataSourceTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc

	dir      string
	addr     string
	syncAddr string
}

var _ = Suite(&DataSourceTestSuite{})

func (s *DataSourceTestSuite) SetUpTest(c *C) {
	logrus.SetLevel(logrus.DebugLevel)

	err := os.Setenv(util.EnvPodIP, "localhost")
	c.Assert(err, IsNil)

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.addr = fmt.Sprintf("localhost:%d", TestDataSourceServerPort)
	s.syncAddr = fmt.Sprintf("localhost:%d", TestSyncServerPort)

	err = checkAndWaitForServer(s.addr, s.syncAddr, 5, false)
	c.Assert(err, IsNil)

	s.dir, err = prepareTestWorkDirectory()
	c.Assert(err, IsNil)
	err = generateDiskConfigFile(s.dir)
	c.Assert(err, IsNil)
}

func (s *DataSourceTestSuite) TearDownTest(c *C) {
	if s.dir != "" {
		os.RemoveAll(s.dir)
	}

	s.cancel()
	err := checkAndWaitForServer(s.addr, s.syncAddr, 5, false)
	c.Assert(err, IsNil)
}

func (s *DataSourceTestSuite) BenchmarkDownload(c *C) {
	biName := "data-source-download-file"

	go NewServer(s.ctx, s.addr, s.syncAddr, "", string(types.DataSourceTypeDownload), biName, TestBackingImageUUID, s.dir,
		map[string]string{types.DataSourceTypeDownloadParameterURL: "http://mock-download"},
		&sync.MockHandler{})
	err := checkAndWaitForServer(s.addr, s.syncAddr, 5, true)
	c.Assert(err, IsNil)

	c.ResetTimer()
	cli := &client.DataSourceClient{
		Remote: s.addr,
	}

	_, err = getAndWaitFileState(cli, string(types.StateReadyForTransfer), 30)
	c.Assert(err, IsNil)

	downloadedFilePath := types.GetDataSourceFilePath(s.dir, biName, TestBackingImageUUID)
	stat, err := os.Stat(downloadedFilePath)
	c.Assert(err, IsNil)
	c.Assert(stat.Size(), Equals, MockFileSize)

}

func (s *DataSourceTestSuite) BenchmarkUpload(c *C) {
	biName := "data-source-upload-file"
	originalFileName := "data-source-original-file"
	originalFilePath := filepath.Join(s.dir, originalFileName)

	logrus.Debugf("preparing the upload file %v for the test, this may be time-consuming", originalFilePath)
	err := generateRandomDataFile(originalFilePath, 1024)
	c.Assert(err, IsNil)
	logrus.Debugf("the upload file %v for the test is ready", originalFilePath)

	checksum, err := util.GetFileChecksum(originalFilePath)
	c.Assert(err, IsNil)

	// Test if the proxy works
	go NewServer(s.ctx, s.addr, s.syncAddr, checksum, string(types.DataSourceTypeUpload), biName, TestBackingImageUUID, s.dir, map[string]string{"fileType": types.SyncingFileTypeQcow2}, &sync.HTTPHandler{})
	err = checkAndWaitForServer(s.addr, s.syncAddr, 5, true)
	c.Assert(err, IsNil)

	c.ResetTimer()
	cli := &client.DataSourceClient{
		Remote: s.addr,
	}

	// The ds file state should be marked as pending before the actual upload starts.
	_, err = getAndWaitFileState(cli, string(types.StatePending), 1)
	c.Assert(err, IsNil)

	go func() {
		err := cli.Upload(originalFilePath)
		c.Assert(err, IsNil)
	}()

	_, err = getAndWaitFileState(cli, string(types.StateReadyForTransfer), 60)
	c.Assert(err, IsNil)

	uploadedFilePath := types.GetDataSourceFilePath(s.dir, biName, TestBackingImageUUID)
	err = exec.Command("diff", originalFilePath, uploadedFilePath).Run()
	c.Assert(err, IsNil)
}

func (s *DataSourceTestSuite) TestTimeoutExportingFromVolume(c *C) {
	biName := "data-source-file-timeout-exporting"

	parameters := map[string]string{
		types.DataSourceTypeFileType:                               types.SyncingFileTypeQcow2,
		types.DataSourceTypeExportFromVolumeParameterSenderAddress: "invalid-addr",
		types.DataSourceTypeExportFromVolumeParameterSnapshotName:  "invalid-snap",
	}
	go NewServer(s.ctx, s.addr, s.syncAddr, "", string(types.DataSourceTypeExportFromVolume), biName, TestBackingImageUUID, s.dir,
		parameters, &sync.HTTPHandler{})
	err := checkAndWaitForServer(s.addr, s.syncAddr, 5, true)
	c.Assert(err, IsNil)

	cli := &client.DataSourceClient{
		Remote: s.addr,
	}

	// dsInfo.State should get stuck in starting for 60s since there is no valid sender
	_, err = getAndWaitFileState(cli, string(types.StateStarting), 5)
	c.Assert(err, IsNil)

	// Sync service timeout waiting for the state becoming in-progress
	_, err = getAndWaitFileState(cli, string(types.StateFailed), 65)
	c.Assert(err, IsNil)

	// Transferring a failed ds file should fail
	err = cli.Transfer()
	c.Assert(err, NotNil)
}

func (s *DataSourceTestSuite) TestTransfer(c *C) {
	biName := "data-source-download-file-transfer"
	downloadedFilePath := types.GetDataSourceFilePath(s.dir, biName, TestBackingImageUUID)

	go NewServer(s.ctx, s.addr, s.syncAddr, "", string(types.DataSourceTypeDownload), biName, TestBackingImageUUID, s.dir,
		map[string]string{types.DataSourceTypeDownloadParameterURL: "http://mock-download"},
		&sync.MockHandler{})
	err := checkAndWaitForServer(s.addr, s.syncAddr, 5, true)
	c.Assert(err, IsNil)

	cli := &client.DataSourceClient{
		Remote: s.addr,
	}

	_, err = getAndWaitFileState(cli, string(types.StateReadyForTransfer), 30)
	c.Assert(err, IsNil)

	stat, err := os.Stat(downloadedFilePath)
	c.Assert(err, IsNil)
	c.Assert(stat.Size(), Equals, int64(MockFileSize))

	// dsInfo.State should become ready after transferring
	err = cli.Transfer()
	c.Assert(err, IsNil)
	_, err = getAndWaitFileState(cli, string(types.StateReady), 1)
	c.Assert(err, IsNil)

	stat, err = os.Stat(downloadedFilePath)
	c.Assert(err, IsNil)
	c.Assert(stat.Size(), Equals, int64(MockFileSize))
}

func getAndWaitFileState(cli *client.DataSourceClient, desireState string, waitIntervalInSecond int) (dsInfo *api.DataSourceInfo, err error) {
	endTime := time.Now().Add(time.Duration(waitIntervalInSecond) * time.Second)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for time.Now().Before(endTime) {
		<-ticker.C

		dsInfo, err = cli.Get()
		if err == nil && dsInfo.State == desireState {
			return dsInfo, nil
		}
		if dsInfo != nil {
			logrus.Debugf("Waiting for datasource file %v(%v) becoming state %v, current state %v, progress %v%% (%v Byte), file path %v",
				dsInfo.Name, dsInfo.UUID, desireState, dsInfo.State, dsInfo.Progress, dsInfo.ProcessedSize, dsInfo.FilePath)
		} else {
			logrus.Warnf("Failed to get datasource file info during wait: %v", err)
		}
	}

	if dsInfo != nil {
		return nil, fmt.Errorf("failed to wait for datasource file %v(%v) to become state %v within %v second, "+
			"current state: %v, progress %v, file path %v, current checksum %v, message: %v",
			dsInfo.Name, dsInfo.UUID, desireState, waitIntervalInSecond,
			dsInfo.State, dsInfo.Progress, dsInfo.FilePath, dsInfo.CurrentChecksum, dsInfo.Message)
	}
	return nil, fmt.Errorf("failed to wait for datasource file to become state %v within %v second, no datesource file info",
		desireState, waitIntervalInSecond)
}

func checkAndWaitForServer(addr, syncAddr string, waitIntervalInSecond int, shouldAvailable bool) error {
	if !util.DetectHTTPServerAvailability("http://"+addr, waitIntervalInSecond, shouldAvailable) {
		return fmt.Errorf("failed to wait for the datasource service becoming %v in %v second", shouldAvailable, waitIntervalInSecond)
	}
	if !util.DetectHTTPServerAvailability("http://"+syncAddr, waitIntervalInSecond, shouldAvailable) {
		return fmt.Errorf("failed to wait for the embedded sync service becoming %v in %v second", shouldAvailable, waitIntervalInSecond)
	}
	return nil
}

func prepareTestWorkDirectory() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	parentDir := filepath.Join(currentUser.HomeDir, "test-dir")
	if err = os.MkdirAll(parentDir, 0666); err != nil && !os.IsExist(err) {
		return "", err
	}

	dir := filepath.Join(parentDir, "datasource-tests")
	if err = os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err = os.MkdirAll(dir, 0666); err != nil {
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
		err = os.WriteFile(diskCfgPath, encodedDiskCfg, 0777)
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

func generateRandomDataFile(filePath string, sizeInMB int) error {
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return exec.Command("dd", "if=/dev/urandom", "of="+filePath, "bs=1M", "count="+strconv.Itoa(sizeInMB)).Run()
}
