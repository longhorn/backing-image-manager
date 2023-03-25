package sync

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/api"

	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"

	. "gopkg.in/check.v1"
)

const (
	TestSyncingFileUUID = "test-sf-uuid"
	TestDiskUUID        = "test-disk-uuid"

	// TestSyncServerPort should be different from the port used in other packages like `datasource`.
	// Since `go test` runs in parallel tests for different packages by default, and default is the number of CPUs available.
	TestSyncServerPort         = 8001
	TestSyncServiceReceivePort = 8002

	MB = 1 << 20
)

func Test(t *testing.T) { TestingT(t) }

type SyncTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc

	dir      string
	addr     string
	httpAddr string
}

var _ = Suite(&SyncTestSuite{})

func (s *SyncTestSuite) SetUpTest(c *C) {
	var err error

	logrus.SetLevel(logrus.DebugLevel)

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.addr = fmt.Sprintf("localhost:%d", TestSyncServerPort)
	s.httpAddr = "http://" + s.addr

	isStopped := util.DetectHTTPServerAvailability(s.httpAddr, 5, false)
	c.Assert(isStopped, Equals, true)

	s.dir, err = prepareTestWorkDirectory()
	c.Assert(err, IsNil)
}

func (s *SyncTestSuite) TearDownTest(c *C) {
	if s.dir != "" {
		os.RemoveAll(s.dir)
	}

	s.cancel()
	isStopped := util.DetectHTTPServerAvailability(s.httpAddr, 5, false)
	c.Assert(isStopped, Equals, true)
}

func (s *SyncTestSuite) BenchmarkMultipleDownload(c *C) {
	logrus.Debugf("Testing sync server: BenchmarkMultipleDownload")

	fileName := "sync-download-file"
	downloadedFilePathBase := filepath.Join(s.dir, fileName)

	go NewServer(s.ctx, s.addr, &MockHandler{})
	isRunning := util.DetectHTTPServerAvailability(s.httpAddr, 5, true)
	c.Assert(isRunning, Equals, true)

	c.ResetTimer()

	concurrency := 100
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		curPath := downloadedFilePathBase + "-" + strconv.Itoa(i)
		curUUID := TestSyncingFileUUID + "-" + strconv.Itoa(i)
		go func() {
			defer wg.Done()

			cli := &client.SyncClient{
				Remote: s.addr,
			}

			err := cli.DownloadFromURL("http://test-download-from-url.io", curPath, curUUID, TestDiskUUID, "")
			c.Assert(err, IsNil)

			_, err = getAndWaitFileState(cli, curPath, string(types.StateReady), 30)
			c.Assert(err, IsNil)

			stat, err := os.Stat(curPath)
			c.Assert(err, IsNil)
			c.Assert(stat.Size(), Equals, int64(MockFileSize))

			err = cli.Forget(curPath)
			c.Assert(err, IsNil)

			_, err = cli.Get(curPath)
			c.Assert(util.IsHTTPClientErrorNotFound(err), Equals, true)
		}()
	}
	wg.Wait()
}

func (s *SyncTestSuite) BenchmarkUpload(c *C) {
	logrus.Debugf("Testing sync server: BenchmarkUpload")

	fileName := "sync-upload-file"
	originalFileName := "sync-original-file"
	originalFilePath := filepath.Join(s.dir, originalFileName)
	uploadedFilePathBase := filepath.Join(s.dir, fileName)

	logrus.Debugf("preparing the upload file %v for the test, this may be time-consuming", originalFilePath)
	err := generateRandomDataFile(originalFilePath, "1024")
	c.Assert(err, IsNil)
	logrus.Debugf("the upload file %v for the test is ready", originalFilePath)

	expectedChecksum, err := util.GetFileChecksum(originalFilePath)
	c.Assert(err, IsNil)

	go NewServer(s.ctx, s.addr, &MockHandler{})
	isRunning := util.DetectHTTPServerAvailability(s.httpAddr, 5, true)
	c.Assert(isRunning, Equals, true)

	c.ResetTimer()

	concurrency := 5
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		curPath := uploadedFilePathBase + "-" + strconv.Itoa(i)
		curUUID := TestSyncingFileUUID + "-" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			cli := &client.SyncClient{
				Remote: s.addr,
			}

			go func() {
				err := cli.Upload(originalFilePath, curPath, curUUID, TestDiskUUID, expectedChecksum)
				c.Assert(err, IsNil)
			}()

			_, err := getAndWaitFileState(cli, curPath, string(types.StateReady), 300)
			c.Assert(err, IsNil)

			err = cli.Delete(curPath)
			c.Assert(err, IsNil)
		}()
	}

	wg.Wait()
}

func (s *SyncTestSuite) BenchmarkOneReceiveAndMultiSendWithSendingLimit(c *C) {
	logrus.Debugf("Testing sync server: BenchmarkOneReceiveAndMultiSendWithSendingLimit")

	originalFilePath := filepath.Join(s.dir, "sync-original-file")
	dstFilePathBase := filepath.Join(s.dir, "sync-dst-file-")

	sizeInMB := 512
	logrus.Debugf("preparing the original file %v for the test, this may be time-consuming", originalFilePath)
	err := generateRandomDataFile(originalFilePath, strconv.Itoa(sizeInMB))
	c.Assert(err, IsNil)
	logrus.Debugf("the original file %v for the test is ready", originalFilePath)

	checksum, err := util.GetFileChecksum(originalFilePath)
	c.Assert(err, IsNil)
	c.Assert(checksum, Not(Equals), "")

	go NewServer(s.ctx, s.addr, &MockHandler{})
	if !util.DetectHTTPServerAvailability(s.httpAddr, 5, true) {
		logrus.Fatal("failed to wait for sync service running in 5 second")
	}

	cli := &client.SyncClient{
		Remote: s.addr,
	}
	err = cli.Fetch(originalFilePath, originalFilePath, TestSyncingFileUUID, TestDiskUUID, checksum, int64(sizeInMB*MB))
	c.Assert(err, IsNil)
	_, err = getAndWaitFileState(cli, originalFilePath, string(types.StateReady), 60)
	c.Assert(err, IsNil)

	concurrency := types.SendingLimit * 2

	wg := sync.WaitGroup{}
	wg.Add(types.SendingLimit)

	c.ResetTimer()
	for i := 0; i < concurrency; i++ {
		dstFilePath := dstFilePathBase + strconv.Itoa(i)
		curUUID := TestSyncingFileUUID + "-dst-" + strconv.Itoa(i)
		curReceiverPort := TestSyncServiceReceivePort + i
		curReceiverAddress := fmt.Sprintf("localhost:%d", curReceiverPort)
		err := cli.Receive(dstFilePath, curUUID, TestDiskUUID, checksum, types.SyncingFileTypeQcow2, curReceiverPort, int64(sizeInMB*MB))
		c.Assert(err, IsNil)

		err = cli.Send(originalFilePath, curReceiverAddress)
		if i < types.SendingLimit {
			c.Assert(err, IsNil)
			go func() {
				defer wg.Done()
				_, err = getAndWaitFileState(cli, dstFilePath, string(types.StateReady), 300)
				c.Assert(err, IsNil)
				err = cli.Delete(dstFilePath)
				c.Assert(err, IsNil)
			}()
		} else {
			c.Assert(err, ErrorMatches, `.*reaches to the simultaneous sending limit[\s\S]*`)
			err = cli.Delete(dstFilePath)
			c.Assert(err, IsNil)

			fInfo, err := cli.Get(originalFilePath)
			c.Assert(err, IsNil)
			c.Assert(fInfo.SendingReference >= types.SendingLimit, Equals, true)
		}
	}

	wg.Wait()

	err = cli.Delete(originalFilePath)
	c.Assert(err, IsNil)
}

func (s *SyncTestSuite) BenchmarkMultiReceiveAndMultiSend(c *C) {
	logrus.Debugf("Testing sync server: BenchmarkMultiReceiveAndMultiSend")

	originalFilePath := filepath.Join(s.dir, "sync-original-file")
	srcFilePathBase := filepath.Join(s.dir, "sync-src-file-")
	dstFilePathBase := filepath.Join(s.dir, "sync-dst-file-")

	sizeInMB := 512
	logrus.Debugf("preparing the original file %v for the test, this may be time-consuming", originalFilePath)
	err := generateRandomDataFile(originalFilePath, strconv.Itoa(sizeInMB))
	c.Assert(err, IsNil)
	logrus.Debugf("the original file %v for the test is ready", originalFilePath)

	checksum, err := util.GetFileChecksum(originalFilePath)
	c.Assert(err, IsNil)
	c.Assert(checksum, Not(Equals), "")

	go NewServer(s.ctx, s.addr, &MockHandler{})
	if !util.DetectHTTPServerAvailability(s.httpAddr, 5, true) {
		logrus.Fatal("failed to wait for sync service running in 5 second")
	}

	concurrency := 3
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	c.ResetTimer()
	for i := 0; i < concurrency; i++ {
		srcFilePath := srcFilePathBase + strconv.Itoa(i)
		curUUID := TestSyncingFileUUID + "-src-" + strconv.Itoa(i)
		go func() {
			defer wg.Done()

			cli := &client.SyncClient{
				Remote: s.addr,
			}

			// Copy file
			_, err := util.CopyFile(originalFilePath, srcFilePath)
			c.Assert(err, IsNil)

			err = cli.Fetch(srcFilePath, srcFilePath, curUUID, TestDiskUUID, checksum, int64(sizeInMB*MB))
			c.Assert(err, IsNil)

			_, err = getAndWaitFileState(cli, srcFilePath, string(types.StateReady), 300)
			c.Assert(err, IsNil)
		}()
	}

	wg.Wait()
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		srcFilePath := srcFilePathBase + strconv.Itoa(i)
		dstFilePath := dstFilePathBase + strconv.Itoa(i)
		curUUID := TestSyncingFileUUID + "-dst-" + strconv.Itoa(i)
		curReceiverPort := TestSyncServiceReceivePort + i
		curReceiverAddress := fmt.Sprintf("localhost:%d", curReceiverPort)
		go func() {
			defer wg.Done()
			cli := &client.SyncClient{
				Remote: s.addr,
			}

			err := cli.Receive(dstFilePath, curUUID, TestDiskUUID, checksum, types.SyncingFileTypeQcow2, curReceiverPort, int64(sizeInMB*MB))
			c.Assert(err, IsNil)
			err = cli.Send(srcFilePath, curReceiverAddress)
			c.Assert(err, IsNil)

			_, err = getAndWaitFileState(cli, dstFilePath, string(types.StateReady), 300)
			c.Assert(err, IsNil)

			err = cli.Delete(srcFilePath)
			c.Assert(err, IsNil)
			err = cli.Delete(dstFilePath)
			c.Assert(err, IsNil)
		}()
	}

	wg.Wait()
}

func (s *SyncTestSuite) TestTimeoutReceiveFromPeers(c *C) {
	logrus.Debugf("Testing sync server: TestTimeoutReceiveFromPeers")

	fileName := "sync-receive-file-timeout"
	curPath := filepath.Join(s.dir, fileName)

	go NewServer(s.ctx, s.addr, &MockHandler{})
	if !util.DetectHTTPServerAvailability(s.httpAddr, 5, true) {
		logrus.Fatal("failed to wait for sync service running in 5 second")
	}

	cli := &client.SyncClient{
		Remote: s.addr,
	}

	go func() {
		err := cli.Receive(curPath, TestSyncingFileUUID, TestDiskUUID, "", types.SyncingFileTypeQcow2, TestSyncServiceReceivePort, MockFileSize)
		c.Assert(err, IsNil)
	}()

	_, err := getAndWaitFileState(cli, curPath, string(types.StateStarting), 5)
	c.Assert(err, IsNil)

	_, err = getAndWaitFileState(cli, curPath, string(types.StateFailed), 65)
	c.Assert(err, IsNil)

	err = cli.Delete(curPath)
	c.Assert(err, IsNil)
}

func (s *SyncTestSuite) TestFetch(c *C) {
	logrus.Debugf("Testing sync server: TestFetch")

	fileName := "sync-fetch-file"
	curPath := filepath.Join(s.dir, fileName)
	sizeInMB := int64(1024)

	logrus.Debugf("preparing the fetch file %v for the test, this may be time-consuming", curPath)
	err := generateRandomDataFile(curPath, strconv.FormatInt(sizeInMB, 10))
	c.Assert(err, IsNil)
	logrus.Debugf("the fetch file %v for the test is ready", curPath)

	checksum, err := util.GetFileChecksum(curPath)
	c.Assert(err, IsNil)
	c.Assert(checksum, Not(Equals), "")

	go NewServer(s.ctx, s.addr, &MockHandler{})
	isRunning := util.DetectHTTPServerAvailability(s.httpAddr, 5, true)
	c.Assert(isRunning, Equals, true)

	cli := &client.SyncClient{
		Remote: s.addr,
	}

	// Reusing the existing file without the config file will take some time.
	err = cli.Fetch(curPath, curPath, TestSyncingFileUUID, TestDiskUUID, checksum, sizeInMB*MB)
	c.Assert(err, IsNil)
	_, err = getAndWaitFileState(cli, curPath, string(types.StateReady), 60)
	c.Assert(err, IsNil)
	err = cli.Forget(curPath)
	c.Assert(err, IsNil)

	// Reusing the existing file with the config file would skip the checksum calculation.
	err = cli.Fetch(curPath, curPath, TestSyncingFileUUID, TestDiskUUID, checksum, sizeInMB*MB)
	c.Assert(err, IsNil)
	_, err = getAndWaitFileState(cli, curPath, string(types.StateReady), 3)
	c.Assert(err, IsNil)
	err = cli.Forget(curPath)
	c.Assert(err, IsNil)

	// Prepare to fetch the file to a new directory
	secondDir := filepath.Join(s.dir, "second-fetch-dir")
	secondPath := filepath.Join(secondDir, fileName)
	err = os.MkdirAll(secondDir, 0666)
	c.Assert(err, IsNil)
	defer os.RemoveAll(secondDir)

	// Moving the existing file with the config file to another place would skip the checksum calculation.
	err = cli.Fetch(curPath, secondPath, TestSyncingFileUUID, TestDiskUUID, checksum, sizeInMB*MB)
	c.Assert(err, IsNil)
	_, err = getAndWaitFileState(cli, secondPath, string(types.StateReady), 3)
	c.Assert(err, IsNil)
	err = cli.Forget(secondPath)
	c.Assert(err, IsNil)

	// Moving the existing file without the config file to another place would take some time.
	err = os.RemoveAll(util.GetSyncingFileConfigFilePath(secondPath))
	c.Assert(err, IsNil)
	err = cli.Fetch(secondPath, curPath, TestSyncingFileUUID, TestDiskUUID, checksum, sizeInMB*MB)
	c.Assert(err, IsNil)
	_, err = getAndWaitFileState(cli, curPath, string(types.StateReady), 60)
	c.Assert(err, IsNil)

	err = cli.Delete(curPath)
	c.Assert(err, IsNil)
	_, err = os.Stat(curPath)
	c.Assert(os.IsNotExist(err), Equals, true)
	_, err = os.Stat(util.GetSyncingFileConfigFilePath(curPath))
	c.Assert(os.IsNotExist(err), Equals, true)
	_, err = os.Stat(secondPath)
	c.Assert(os.IsNotExist(err), Equals, true)
	_, err = os.Stat(util.GetSyncingFileConfigFilePath(secondPath))
	c.Assert(os.IsNotExist(err), Equals, true)

	// The file is already removed.
	go func() {
		err := cli.Fetch(curPath, curPath, TestSyncingFileUUID, TestDiskUUID, checksum, sizeInMB*MB)
		c.Assert(err, IsNil)
	}()
	_, err = getAndWaitFileState(cli, curPath, string(types.StateFailed), 65)
	c.Assert(err, IsNil)

	err = cli.Delete(curPath)
	c.Assert(err, IsNil)
}

func (s *SyncTestSuite) TestDownloadToDst(c *C) {
	logrus.Debugf("Testing sync server: TestDownloadToDst")

	fileName := "sync-download-src-file"
	curPath := filepath.Join(s.dir, fileName)
	sizeInMB := int64(1024)

	logrus.Debugf("preparing the src file %v with format qcow2 for the test, this may be time-consuming", curPath)
	err := generateRandomDataFile(curPath, strconv.FormatInt(sizeInMB, 10))
	c.Assert(err, IsNil)
	err = util.ConvertFromRawToQcow2(curPath)
	c.Assert(err, IsNil)
	// File size will change after the conversion.
	stat, err := os.Stat(curPath)
	c.Assert(err, IsNil)
	logrus.Debugf("the src file %v with format qcow2 for the test is ready", curPath)

	checksum, err := util.GetFileChecksum(curPath)
	c.Assert(err, IsNil)
	c.Assert(checksum, Not(Equals), "")

	go NewServer(s.ctx, s.addr, &HTTPHandler{})
	isRunning := util.DetectHTTPServerAvailability(s.httpAddr, 5, true)
	c.Assert(isRunning, Equals, true)

	cli := &client.SyncClient{
		Remote: s.addr,
	}

	err = cli.Fetch(curPath, curPath, TestSyncingFileUUID, TestDiskUUID, checksum, stat.Size())
	c.Assert(err, IsNil)
	_, err = getAndWaitFileState(cli, curPath, string(types.StateReady), 60)
	c.Assert(err, IsNil)

	downloadFileName := "sync-download-dst-file"
	downloadFilePath := filepath.Join(s.dir, downloadFileName)
	err = cli.DownloadToDst(curPath, downloadFilePath)
	c.Assert(err, IsNil)

	downloadChecksum, err := util.GetFileChecksum(downloadFilePath)
	c.Assert(err, IsNil)
	c.Assert(downloadChecksum, Equals, checksum)
	// Downloaded file can be identified as a qcow2 file as well.
	downloadFileFormat, err := util.DetectFileFormat(downloadFilePath)
	c.Assert(err, IsNil)
	c.Assert(downloadFileFormat, Equals, "qcow2")
}

func (s *SyncTestSuite) TestDuplicateCalls(c *C) {
	logrus.Debugf("Testing sync server: TestDuplicateCalls")

	fileName := "sync-download-file-for-dup-calls"
	curPath := filepath.Join(s.dir, fileName)

	go NewServer(s.ctx, s.addr, &MockHandler{})
	isRunning := util.DetectHTTPServerAvailability(s.httpAddr, 5, true)
	c.Assert(isRunning, Equals, true)

	cli := &client.SyncClient{
		Remote: s.addr,
	}

	err := cli.DownloadFromURL("http://test-download-from-url.io", curPath, TestSyncingFileUUID, TestDiskUUID, "")
	c.Assert(err, IsNil)

	_, err = getAndWaitFileState(cli, curPath, string(types.StateReady), 30)
	c.Assert(err, IsNil)

	// Duplicate file launching calls should error out:
	// "resp.StatusCode(500) != http.StatusOK(200), response body content: file /root/test-dir/sync-tests/sync-download-file-for-dup-calls already exists\n"
	err = cli.DownloadFromURL("http://test-download-from-url.io", curPath, TestSyncingFileUUID, TestDiskUUID, "")
	c.Assert(err, ErrorMatches, `.*already exists[\s\S]*`)
	err = cli.Upload(curPath, curPath, TestSyncingFileUUID, TestDiskUUID, "")
	c.Assert(err, ErrorMatches, `.*already exists[\s\S]*`)
	err = cli.Receive(curPath, TestDiskUUID, TestSyncingFileUUID, "", "", types.DefaultVolumeExportReceiverPort, MockFileSize)
	c.Assert(err, ErrorMatches, `.*already exists[\s\S]*`)
	err = cli.DownloadFromURL("http://test-download-from-url.io", curPath+"-non-existing", TestSyncingFileUUID, TestDiskUUID, "")
	c.Assert(err, ErrorMatches, `.*already exists[\s\S]*`)

	// Duplicate delete or forget calls won't error out
	for i := 0; i < 10; i++ {
		err = cli.Delete(curPath)
		c.Assert(err, IsNil)
		err = cli.Forget(curPath)
		c.Assert(err, IsNil)
	}
}

func (s *SyncTestSuite) TestForgetFile(c *C) {
	logrus.Debugf("Testing sync server: TestForgetFile")

	fileName := "sync-upload-file-for-forget"
	originalFileName := "sync-original-file-for-forget"
	originalFilePath := filepath.Join(s.dir, originalFileName)
	curPath := filepath.Join(s.dir, fileName)

	err := generateRandomDataFile(originalFilePath, "4")
	c.Assert(err, IsNil)

	expectedChecksum, err := util.GetFileChecksum(originalFilePath)
	c.Assert(err, IsNil)

	go NewServer(s.ctx, s.addr, &MockHandler{})
	isRunning := util.DetectHTTPServerAvailability(s.httpAddr, 5, true)
	c.Assert(isRunning, Equals, true)

	cli := &client.SyncClient{
		Remote: s.addr,
	}

	go func() {
		err := cli.Upload(originalFilePath, curPath, TestSyncingFileUUID, TestDiskUUID, expectedChecksum)
		c.Assert(err, IsNil)
	}()

	fInfo, err := getAndWaitFileState(cli, curPath, string(types.StateReady), 30)
	c.Assert(err, IsNil)

	err = cli.Forget(curPath)
	c.Assert(err, IsNil)

	// Delete calls after the forget call don't make sense. The file should exist.
	err = cli.Delete(curPath)
	c.Assert(err, IsNil)

	currentChecksum, err := util.GetFileChecksum(curPath)
	c.Assert(err, IsNil)
	c.Assert(fInfo.CurrentChecksum, Equals, currentChecksum)
}

func (s *SyncTestSuite) TestReadyFileValidation(c *C) {
	logrus.Debugf("Testing sync server: TestDuplicateCalls")

	fileName := "sync-download-file-ready-validation"
	curPath := filepath.Join(s.dir, fileName)

	go NewServer(s.ctx, s.addr, &MockHandler{})
	isRunning := util.DetectHTTPServerAvailability(s.httpAddr, 5, true)
	c.Assert(isRunning, Equals, true)

	cli := &client.SyncClient{
		Remote: s.addr,
	}

	err := cli.DownloadFromURL("http://test-download-from-url.io", curPath, TestSyncingFileUUID, TestDiskUUID, "")
	c.Assert(err, IsNil)

	fInfo, err := getAndWaitFileState(cli, curPath, string(types.StateReady), 30)
	c.Assert(err, IsNil)
	oldModificationTime := fInfo.ModificationTime

	f, err := os.OpenFile(curPath, os.O_RDWR, 0666)
	c.Assert(err, IsNil)
	defer f.Close()

	// Change the file modification time without affecting the data.
	// File state should be state unknown with a different modification time,
	// then be back to state ready.
	content, err := io.ReadAll(f)
	c.Assert(err, IsNil)
	_, err = f.WriteAt([]byte("invalid data"), 0)
	c.Assert(err, IsNil)
	_, err = f.WriteAt(content, 0)
	c.Assert(err, IsNil)
	fInfo, err = getAndWaitFileState(cli, curPath, string(types.StateUnknown), 1)
	c.Assert(err, IsNil)
	c.Check(fInfo.ModificationTime == oldModificationTime, Equals, false)
	fInfo, err = getAndWaitFileState(cli, curPath, string(types.StateReady), 10)
	c.Assert(err, IsNil)
	oldModificationTime = fInfo.ModificationTime

	// Change the file modification time and the data.
	// File state should be state unknown with a different modification time,
	// then fall into state failed.
	_, err = f.WriteAt([]byte("invalid data"), 0)
	c.Assert(err, IsNil)
	fInfo, err = getAndWaitFileState(cli, curPath, string(types.StateUnknown), 1)
	c.Assert(err, IsNil)
	c.Check(fInfo.ModificationTime == oldModificationTime, Equals, false)
	fInfo, err = getAndWaitFileState(cli, curPath, string(types.StateFailed), 10)
	c.Assert(err, IsNil)

	err = cli.Delete(curPath)
	c.Assert(err, IsNil)
}

func getAndWaitFileState(cli *client.SyncClient, curPath, desireState string, waitIntervalInSecond int) (fInfo *api.FileInfo, err error) {
	endTime := time.Now().Add(time.Duration(waitIntervalInSecond) * time.Second)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for time.Now().Before(endTime) {
		<-ticker.C

		fInfo, err = cli.Get(curPath)
		if err != nil || fInfo == nil {
			logrus.Warnf("Failed to get file %v info during wait: %v", curPath, err)
			continue
		}

		if fInfo.State != desireState {
			logrus.Debugf("Waiting for file %v becoming state %v, current state %v, progress %v%% (%v Byte)",
				curPath, desireState, fInfo.State, fInfo.Progress, fInfo.ProcessedSize)
			continue
		}

		if fInfo.State == string(types.StateReady) {
			config, err := util.ReadSyncingFileConfig(util.GetSyncingFileConfigFilePath(curPath))
			if err != nil {
				return nil, fmt.Errorf("failed to get the config file after waiting for the file becoming state %v: %v", types.StateReady, err)
			}
			fInfoConfig := util.SyncingFileConfig{
				FilePath:         fInfo.FilePath,
				UUID:             fInfo.UUID,
				Size:             fInfo.Size,
				ExpectedChecksum: fInfo.ExpectedChecksum,
				CurrentChecksum:  fInfo.CurrentChecksum,
				ModificationTime: fInfo.ModificationTime,
			}
			if !reflect.DeepEqual(*config, fInfoConfig) {
				return nil, fmt.Errorf("the file config %+v does not match the file info %+v after waiting for the file becoming state %v: %v", *config, fInfoConfig, types.StateReady, err)
			}
		}
		return fInfo, nil
	}

	if fInfo != nil {
		return nil, fmt.Errorf("failed to wait for file %v to become state %v within %v second, "+
			"current state: %v, progress %v, current checksum %v, message: %v",
			curPath, desireState, waitIntervalInSecond,
			fInfo.State, fInfo.Progress, fInfo.CurrentChecksum, fInfo.Message)
	}
	return nil, fmt.Errorf("failed to wait for datasource file to become state %v within %v second, no file info",
		desireState, waitIntervalInSecond)
}

func prepareTestWorkDirectory() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(currentUser.HomeDir, "test-dir", "sync-tests")
	if err = os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err = os.MkdirAll(dir, 0666); err != nil {
		return "", err
	}

	return dir, nil
}

func generateRandomDataFile(filePath, sizeInMB string) error {
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

	return exec.Command("dd", "if=/dev/urandom", "of="+filePath, "bs=1M", "count="+sizeInMB).Run()
}
