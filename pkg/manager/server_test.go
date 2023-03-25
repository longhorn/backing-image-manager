package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/datasource"
	filesync "github.com/longhorn/backing-image-manager/pkg/sync"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"

	. "gopkg.in/check.v1"
)

const (
	MB           = 1 << 20
	MockFileSize = 4096

	TestDiskUUID1        = "bim-test-disk-cfg-1"
	TestDiskUUID2        = "bim-test-disk-cfg-2"
	TestBackingImageUUID = "test-backing-image-uuid"

	TestManagerServerPort1 = 8200
	TestSyncServerPort1    = 8201
	TestManagerServerPort2 = 8202
	TestSyncServerPort2    = 8203
	TestFirstReservedPort  = 8204
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc

	addr1     string
	syncAddr1 string
	addr2     string
	syncAddr2 string

	testDiskPath1 string
	testBIMDir1   string
	testDiskPath2 string
	testBIMDir2   string
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) prepareDirs(c *C) {
	currentUser, err := user.Current()
	c.Assert(err, IsNil)

	parentDir := filepath.Join(currentUser.HomeDir, "test-dir")
	err = os.Mkdir(parentDir, 0777)
	c.Assert(err != nil && !os.IsExist(err), Equals, false)

	// 1st manager dir
	s.testDiskPath1 = filepath.Join(parentDir, "manager1")
	err = os.RemoveAll(s.testDiskPath1)
	c.Assert(err, IsNil)
	s.testBIMDir1 = filepath.Join(s.testDiskPath1, types.BackingImageManagerDirectoryName)
	err = os.MkdirAll(s.testBIMDir1, 0777)
	c.Assert(err, IsNil)

	diskCfgPath1 := filepath.Join(s.testDiskPath1, util.DiskConfigFile)
	diskCfg1 := &util.DiskConfig{
		DiskUUID: TestDiskUUID1,
	}
	encodedDiskCfg1, err := json.Marshal(diskCfg1)
	c.Assert(err, IsNil)
	err = os.WriteFile(diskCfgPath1, encodedDiskCfg1, 0777)
	c.Assert(err, IsNil)

	// 2nd manager dir
	s.testDiskPath2 = filepath.Join(parentDir, "manager2")
	err = os.RemoveAll(s.testDiskPath2)
	c.Assert(err, IsNil)
	s.testBIMDir2 = filepath.Join(s.testDiskPath2, types.BackingImageManagerDirectoryName)
	err = os.MkdirAll(s.testBIMDir2, 0777)
	c.Assert(err, IsNil)

	diskCfgPath2 := filepath.Join(s.testDiskPath2, util.DiskConfigFile)
	diskCfg2 := &util.DiskConfig{
		DiskUUID: TestDiskUUID2,
	}
	encodedDiskCfg2, err := json.Marshal(diskCfg2)
	c.Assert(err, IsNil)
	err = os.WriteFile(diskCfgPath2, encodedDiskCfg2, 0777)
	c.Assert(err, IsNil)
}

func (s *TestSuite) SetUpSuite(c *C) {
	logrus.SetLevel(logrus.DebugLevel)

	err := os.Setenv(util.EnvPodIP, "localhost")
	c.Assert(err, IsNil)

	s.prepareDirs(c)

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.addr1 = fmt.Sprintf("localhost:%d", TestManagerServerPort1)
	s.syncAddr1 = fmt.Sprintf("localhost:%d", TestSyncServerPort1)
	go NewServer(s.ctx, s.addr1, s.syncAddr1, TestDiskUUID1, s.testDiskPath1, "30001-31000", &filesync.HTTPHandler{})

	s.addr2 = fmt.Sprintf("localhost:%d", TestManagerServerPort2)
	s.syncAddr2 = fmt.Sprintf("localhost:%d", TestSyncServerPort2)
	go NewServer(s.ctx, s.addr2, s.syncAddr2, TestDiskUUID1, s.testDiskPath2, "31001-32000", &filesync.HTTPHandler{})

	err = checkAndWaitForServer(s.addr1, 5, true)
	c.Assert(err, IsNil)
	err = checkAndWaitForServer(s.addr2, 5, true)
	c.Assert(err, IsNil)

}

func (s *TestSuite) TearDownSuite(c *C) {
	if s.cancel != nil {
		s.cancel()
	}
	if s.cancel != nil {
		s.cancel()
	}
	os.RemoveAll(s.testDiskPath1)
	os.RemoveAll(s.testDiskPath2)
}

func (s *TestSuite) TearDownTest(c *C) {
	if s.addr1 != "" {
		cli1 := client.NewBackingImageManagerClient(s.addr1)
		biMap, err := cli1.List()
		if err == nil && biMap != nil {
			for biName, bi := range biMap {
				s.deleteBackingImage(c, s.addr1, s.testDiskPath1, biName, bi.UUID)
			}
		}
	}
	if s.addr2 != "" {
		cli2 := client.NewBackingImageManagerClient(s.addr2)
		biMap, err := cli2.List()
		if err == nil && biMap != nil {
			for biName, bi := range biMap {
				s.deleteBackingImage(c, s.addr2, s.testDiskPath2, biName, bi.UUID)
			}
		}
	}

	if s.testDiskPath1 != "" {
		fList, err := os.ReadDir(s.testDiskPath1)
		c.Assert(err, IsNil)
		for _, fInfo := range fList {
			if fInfo.Name() == types.BackingImageManagerDirectoryName || fInfo.Name() == util.DiskConfigFile {
				continue
			}
			err = os.RemoveAll(filepath.Join(s.testDiskPath1, fInfo.Name()))
			c.Assert(err, IsNil)
		}
	}
	if s.testDiskPath2 != "" {
		fList, err := os.ReadDir(s.testDiskPath2)
		c.Assert(err, IsNil)
		for _, fInfo := range fList {
			if fInfo.Name() == types.BackingImageManagerDirectoryName || fInfo.Name() == util.DiskConfigFile {
				continue
			}
			err = os.RemoveAll(filepath.Join(s.testDiskPath2, fInfo.Name()))
			c.Assert(err, IsNil)
		}
	}
}

func (s *TestSuite) TestMultipleReuseAndSync(c *C) {
	biNameBase := "test-reuse-sync-file-"
	biUUIDBase := TestBackingImageUUID + "-reuse-sync-"
	count := 50
	wg := &sync.WaitGroup{}
	wg.Add(count)

	cli1 := client.NewBackingImageManagerClient(s.addr1)

	for i := 0; i < count; i++ {
		biName := biNameBase + strconv.Itoa(i)
		biUUID := biUUIDBase + strconv.Itoa(i)
		biFilePath := types.GetBackingImageFilePath(s.testDiskPath1, biName, biUUID)
		biFileDir := filepath.Dir(biFilePath)
		err := os.Mkdir(biFileDir, 0777)
		c.Assert(err != nil && !os.IsExist(err), Equals, false)

		go func() {
			defer wg.Done()

			err := generateSimpleTestFile(biFilePath, MockFileSize)
			c.Assert(err, IsNil)
			checksum, err := util.GetFileChecksum(biFilePath)
			c.Assert(err, IsNil)

			bi, err := cli1.Fetch(biName, biUUID, checksum, "", MockFileSize)
			c.Assert(err, IsNil)
			c.Assert(bi.Status.State, Not(Equals), string(types.StateFailed))

			bi, err = getAndWaitFileState(cli1, biName, biUUID, string(types.StateReady), 10)
			c.Assert(err, IsNil)
			c.Assert(bi.Status.CurrentChecksum, Equals, checksum)
			_, err = os.Stat(biFilePath)
			c.Assert(err, IsNil)
		}()
	}

	wg.Wait()
	wg.Add(count)

	// For file syncing
	cli2 := client.NewBackingImageManagerClient(s.addr2)

	for i := 0; i < count; i++ {
		biName := biNameBase + strconv.Itoa(i)
		biUUID := biUUIDBase + strconv.Itoa(i)
		biFilePath := types.GetBackingImageFilePath(s.testDiskPath2, biName, biUUID)

		go func(i int) {
			defer wg.Done()

			bi, err := cli2.Sync(biName, biUUID, "", s.addr1, MockFileSize)
			c.Assert(err, IsNil)
			c.Assert(bi.Status.State, Not(Equals), string(types.StateFailed))

			biMap, err := cli2.List()
			c.Assert(err, IsNil)
			c.Assert(biMap[biName], NotNil)
			c.Assert(biMap[biName].UUID, Equals, biUUID)
			c.Assert(biMap[biName].Status.State, Not(Equals), string(types.StateFailed))

			_, err = getAndWaitFileState(cli2, biName, biUUID, string(types.StateReady), 30)
			c.Assert(err, IsNil)
			_, err = os.Stat(biFilePath)
			c.Assert(err, IsNil)
		}(i)
	}
	wg.Wait()

	for i := 0; i < count; i++ {
		biName := biNameBase + strconv.Itoa(i)
		biUUID := biUUIDBase + strconv.Itoa(i)
		s.deleteBackingImage(c, s.addr1, s.testDiskPath1, biName, biUUID)
		s.deleteBackingImage(c, s.addr2, s.testDiskPath2, biName, biUUID)
	}
}

func (s *TestSuite) TestSingleBackingImageFetch(c *C) {
	biNameBase := "test-single-fetch-file-"
	biUUIDBase := TestBackingImageUUID + "-single-fetch-"

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	cli1 := client.NewBackingImageManagerClient(s.addr1)

	count := 5
	for i := 0; i < count; i++ {
		subCtx, subCancel := context.WithCancel(ctx)

		biName := biNameBase + strconv.Itoa(i)
		biUUID := biUUIDBase + strconv.Itoa(i)
		biFilePath := types.GetBackingImageFilePath(s.testDiskPath1, biName, biUUID)
		err := os.MkdirAll(filepath.Dir(biFilePath), 0777)
		c.Assert(err, IsNil)

		dsFilePath := types.GetDataSourceFilePath(s.testDiskPath1, biName, biUUID)
		err = os.MkdirAll(filepath.Dir(dsFilePath), 0777)
		c.Assert(err, IsNil)

		err = generateSimpleTestFile(dsFilePath, MockFileSize)
		c.Assert(err, IsNil)
		checksum, err := util.GetFileChecksum(dsFilePath)
		c.Assert(err, IsNil)

		dsAddr := fmt.Sprintf("localhost:%d", TestFirstReservedPort+i*2)
		dsSyncAddr := fmt.Sprintf("localhost:%d", TestFirstReservedPort+i*2+1)

		isRunning := launchAndWaitTestDataSourceServer(subCtx, dsAddr, dsSyncAddr, biName, biUUID, checksum, s.testDiskPath1)
		c.Assert(isRunning, Equals, true)

		dsInfo, err := checkAndWaitTestDataSourceFileState(dsAddr, string(types.StateReadyForTransfer), 30)
		c.Assert(err, IsNil)
		c.Assert(dsInfo.UUID, Equals, biUUID)
		c.Assert(dsInfo.FilePath, Equals, dsFilePath)

		_, err = cli1.Fetch(biName, biUUID, checksum, dsAddr, MockFileSize)
		c.Assert(err, IsNil)

		dsInfo, err = checkAndWaitTestDataSourceFileState(dsAddr, string(types.StateReady), 30)
		c.Assert(err, IsNil)
		c.Assert(dsInfo.UUID, Equals, biUUID)
		c.Assert(dsInfo.FilePath, Equals, dsFilePath)

		bi, err := getAndWaitFileState(cli1, biName, biUUID, string(types.StateReady), 30)
		c.Assert(bi.Status.CurrentChecksum, Equals, checksum)

		_, err = os.Stat(dsFilePath)
		c.Assert(os.IsNotExist(err), Equals, true)
		_, err = os.Stat(biFilePath)
		c.Assert(err, IsNil)

		s.deleteBackingImage(c, s.addr1, s.testDiskPath1, biName, biUUID)
		subCancel()
	}
}

func (s *TestSuite) TestSingleBackingImageSync(c *C) {
	biName := "test-single-sync-file"
	biUUID := TestBackingImageUUID
	biFilePath1 := types.GetBackingImageFilePath(s.testDiskPath1, biName, biUUID)

	biFileDir1 := filepath.Dir(biFilePath1)
	err := os.MkdirAll(biFileDir1, 0777)
	c.Assert(err, IsNil)

	sizeInMB := 1024
	size := int64(sizeInMB * MB)
	logrus.Debugf("preparing the original file %v for the test, this may be time-consuming", biFilePath1)
	err = generateRandomDataFile(biFilePath1, sizeInMB)
	c.Assert(err, IsNil)
	defer os.RemoveAll(biFilePath1)
	checksum, err := util.GetFileChecksum(biFilePath1)
	c.Assert(err, IsNil)
	logrus.Debugf("the original file %v for the test is ready", biFilePath1)

	cli1 := client.NewBackingImageManagerClient(s.addr1)
	cli2 := client.NewBackingImageManagerClient(s.addr2)

	// The 1st manager directly reuses the file.
	bi, err := cli1.Fetch(biName, biUUID, checksum, "", size)
	c.Assert(err, IsNil)
	c.Assert(bi.Status.State, Not(Equals), string(types.StateFailed))
	bi, err = getAndWaitFileState(cli1, biName, biUUID, string(types.StateReady), 30)
	c.Assert(err, IsNil)
	c.Assert(bi.Status.CurrentChecksum, Equals, checksum)
	_, err = os.Stat(biFilePath1)
	c.Assert(err, IsNil)
	defer s.deleteBackingImage(c, s.addr1, s.testDiskPath1, biName, biUUID)

	count := 2
	for i := 0; i < count; i++ {
		biFilePath2 := types.GetBackingImageFilePath(s.testDiskPath2, biName, biUUID)

		// The 2nd manager requests/syncs/receives the file from the 1st manager.
		bi, err = cli2.Sync(biName, biUUID, "", s.addr1, size)
		c.Assert(err, IsNil)
		c.Assert(bi.Status.State, Not(Equals), string(types.StateFailed))

		biMap, err := cli2.List()
		c.Assert(err, IsNil)
		c.Assert(biMap[biName], NotNil)
		c.Assert(biMap[biName].UUID, Equals, biUUID)
		c.Assert(biMap[biName].Status.State, Not(Equals), string(types.StateFailed))

		bi, err = getAndWaitFileState(cli2, biName, biUUID, string(types.StateReady), 120)
		c.Assert(err, IsNil)
		c.Assert(bi.Status.CurrentChecksum, Equals, checksum)
		_, err = os.Stat(biFilePath2)
		c.Assert(err, IsNil)

		s.deleteBackingImage(c, s.addr2, s.testDiskPath2, biName, biUUID)
	}
}

func (s *TestSuite) TestBackingImageDownloadToLocal(c *C) {
	biName := "test-download-src-sync-file"
	biUUID := TestBackingImageUUID
	biFilePath1 := types.GetBackingImageFilePath(s.testDiskPath1, biName, biUUID)

	biFileDir1 := filepath.Dir(biFilePath1)
	err := os.MkdirAll(biFileDir1, 0777)
	c.Assert(err, IsNil)

	sizeInMB := 1024
	size := int64(sizeInMB * MB)
	logrus.Debugf("preparing the original file %v for the test, this may be time-consuming", biFilePath1)
	err = generateRandomDataFile(biFilePath1, sizeInMB)
	c.Assert(err, IsNil)
	defer os.RemoveAll(biFilePath1)
	checksum, err := util.GetFileChecksum(biFilePath1)
	c.Assert(err, IsNil)
	logrus.Debugf("the original file %v for the test is ready", biFilePath1)

	cli1 := client.NewBackingImageManagerClient(s.addr1)

	// The 1st manager directly reuses the file.
	bi, err := cli1.Fetch(biName, biUUID, checksum, "", size)
	c.Assert(err, IsNil)
	c.Assert(bi.Status.State, Not(Equals), string(types.StateFailed))
	bi, err = getAndWaitFileState(cli1, biName, biUUID, string(types.StateReady), 30)
	c.Assert(err, IsNil)
	c.Assert(bi.Status.CurrentChecksum, Equals, checksum)
	_, err = os.Stat(biFilePath1)
	c.Assert(err, IsNil)
	defer s.deleteBackingImage(c, s.addr1, s.testDiskPath1, biName, biUUID)

	// Prepare the download
	downloadFilePath := filepath.Join(s.testBIMDir1, "test-download-dst-sync-file")
	srcFilePath, addr, err := cli1.PrepareDownload(biName, biUUID)
	c.Assert(err, IsNil)
	c.Assert(addr, Equals, s.syncAddr1)
	c.Assert(srcFilePath, Equals, biFilePath1)

	// Start the actual download
	syncCli1 := &client.SyncClient{
		Remote: addr,
	}
	err = syncCli1.DownloadToDst(srcFilePath, downloadFilePath)
	c.Assert(err, IsNil)
	defer os.RemoveAll(downloadFilePath)

	downloadChecksum, err := util.GetFileChecksum(downloadFilePath)
	c.Assert(err, IsNil)
	c.Assert(downloadChecksum, Equals, checksum)

	s.deleteBackingImage(c, s.addr1, s.testDiskPath1, biName, biUUID)
}

func (s *TestSuite) TestDuplicateCalls(c *C) {
	biName := "test-duplicate-calls-file"
	biUUID := TestBackingImageUUID
	biFilePath1 := types.GetBackingImageFilePath(s.testDiskPath1, biName, biUUID)

	biFileDir1 := filepath.Dir(biFilePath1)
	err := os.MkdirAll(biFileDir1, 0777)
	c.Assert(err, IsNil)

	sizeInMB := 4
	size := int64(sizeInMB * MB)
	logrus.Debugf("preparing the original file %v for the test, this may be time-consuming", biFilePath1)
	err = generateRandomDataFile(biFilePath1, sizeInMB)
	c.Assert(err, IsNil)
	defer os.RemoveAll(biFilePath1)
	checksum, err := util.GetFileChecksum(biFilePath1)
	c.Assert(err, IsNil)
	logrus.Debugf("the original file %v for the test is ready", biFilePath1)

	cli1 := client.NewBackingImageManagerClient(s.addr1)
	cli2 := client.NewBackingImageManagerClient(s.addr2)

	// The 1st manager directly reuses the file.
	bi, err := cli1.Fetch(biName, biUUID, checksum, "", size)
	c.Assert(err, IsNil)
	c.Assert(bi.Status.State, Not(Equals), string(types.StateFailed))
	bi, err = getAndWaitFileState(cli1, biName, biUUID, string(types.StateReady), 30)
	c.Assert(err, IsNil)
	c.Assert(bi.Status.CurrentChecksum, Equals, checksum)
	_, err = os.Stat(biFilePath1)
	c.Assert(err, IsNil)
	defer s.deleteBackingImage(c, s.addr1, s.testDiskPath1, biName, biUUID)

	retryCount := 3
	for i := 0; i < retryCount; i++ {
		// Allow the 2nd manager to reuse the file via the Fetch call.
		biFilePath2 := types.GetBackingImageFilePath(s.testDiskPath2, biName, biUUID)
		biFileDir2 := filepath.Dir(biFilePath2)
		err = os.MkdirAll(biFileDir2, 0777)
		c.Assert(err, IsNil)
		_, err = util.CopyFile(biFilePath1, biFilePath2)
		c.Assert(err, IsNil)

		duplicateCount := 50

		wg := sync.WaitGroup{}
		wg.Add(duplicateCount * 2)

		lock := &sync.RWMutex{}
		var alreadyExistsErrCount, otherErrCount int
		var finalBI *api.BackingImage

		// Send multiple Fetch or Sync calls simultaneously for one backing image launching.
		for i := 0; i < duplicateCount; i++ {
			go func() {
				defer wg.Done()
				bi, err := cli2.Sync(biName, biUUID, checksum, s.addr1, size)
				lock.Lock()
				defer lock.Unlock()
				if err != nil {
					if strings.Contains(err.Error(), "already exists") {
						alreadyExistsErrCount++
					} else {
						otherErrCount++
					}
				} else if bi != nil && finalBI == nil {
					finalBI = bi
				}
			}()
			go func() {
				defer wg.Done()
				bi, err := cli2.Fetch(biName, biUUID, checksum, "", size)
				lock.Lock()
				defer lock.Unlock()
				if err != nil {
					if strings.Contains(err.Error(), "already exists") {
						alreadyExistsErrCount++
					} else {
						otherErrCount++
					}
				} else if bi != nil && finalBI == nil {
					finalBI = bi
				}
			}()
		}

		wg.Wait()

		c.Assert(alreadyExistsErrCount, Equals, duplicateCount*2-1)
		c.Assert(otherErrCount, Equals, 0)
		c.Assert(finalBI, NotNil)
		c.Assert(finalBI.Status.State, Not(Equals), string(types.StateFailed))

		biMap, err := cli2.List()
		c.Assert(err, IsNil)
		c.Assert(biMap[biName], NotNil)
		c.Assert(biMap[biName].UUID, Equals, biUUID)
		c.Assert(biMap[biName].Status.State, Not(Equals), string(types.StateFailed))

		bi, err = getAndWaitFileState(cli2, biName, biUUID, string(types.StateReady), 30)
		c.Assert(err, IsNil)
		c.Assert(bi.Status.CurrentChecksum, Equals, checksum)
		_, err = os.Stat(biFilePath2)
		c.Assert(err, IsNil)

		s.deleteBackingImage(c, s.addr2, s.testDiskPath2, biName, biUUID)
	}
}

func (s *TestSuite) deleteBackingImage(c *C, addr, diskPath, biName, biUUID string) {
	biFilePath := types.GetBackingImageFilePath(diskPath, biName, biUUID)
	biDir := types.GetBackingImageDirectory(diskPath, biName, biUUID)

	cli := client.NewBackingImageManagerClient(addr)

	err := cli.Delete(biName, biUUID)
	c.Assert(err, IsNil)
	_, err = cli.Get(biName, biUUID)
	c.Assert(err, NotNil)
	c.Assert(status.Code(err), Equals, codes.NotFound)
	_, err = os.Stat(biFilePath)
	c.Assert(os.IsNotExist(err), Equals, true)
	_, err = os.Stat(biDir)
	c.Assert(os.IsNotExist(err), Equals, true)
}

func generateSimpleTestFile(filePath string, size int64) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(size)
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

func getAndWaitFileState(cli *client.BackingImageManagerClient, biName, biUUID, desireState string, waitIntervalInSecond int) (bi *api.BackingImage, err error) {
	for count := 0; count < waitIntervalInSecond; count++ {
		time.Sleep(time.Second)
		bi, err = cli.Get(biName, biUUID)
		if err == nil && bi.Status.State == desireState {
			return bi, nil
		}
		if bi != nil {
			logrus.Debugf("Waiting for backing image file %v(%v) becoming state %v, current state %v, progress %v%%",
				biName, biUUID, desireState, bi.Status.State, bi.Status.Progress)
		} else {
			logrus.Warnf("Failed to get backing image file %v(%v) info during wait: %v", biName, biUUID, err)
		}
	}
	if bi != nil {
		return nil, fmt.Errorf("failed to wait for backing image file %v(%v) to become state %v within %v second, "+
			"current state: %v, progress %v, current checksum %v, message: %v",
			biName, biUUID, desireState, waitIntervalInSecond,
			bi.Status.State, bi.Status.Progress, bi.Status.CurrentChecksum, bi.Status.ErrorMsg)
	}
	return nil, fmt.Errorf("failed to wait for backing image file %v(%v) to become state %v within %v second, no datesource file info",
		biName, biUUID, desireState, waitIntervalInSecond)
}

func checkAndWaitForServer(address string, waitIntervalInSecond int, shouldAvailable bool) error {
	if !util.DetectGRPCServerAvailability(address, waitIntervalInSecond, shouldAvailable) {
		return fmt.Errorf("failed to wait for the manager gRPC service becoming %v in %v second", shouldAvailable, waitIntervalInSecond)
	}
	return nil
}

func launchAndWaitTestDataSourceServer(ctx context.Context, addr, syncAddr, biName, biUUID, checksum, diskPath string) bool {
	go datasource.NewServer(ctx, addr, syncAddr,
		checksum, string(types.DataSourceTypeDownload), biName, biUUID, diskPath,
		map[string]string{types.DataSourceTypeDownloadParameterURL: "http://mock-download"},
		&filesync.MockHandler{},
	)
	return util.DetectHTTPServerAvailability("http://"+addr, 5, true) &&
		util.DetectHTTPServerAvailability("http://"+syncAddr, 5, true)
}

func checkAndWaitTestDataSourceFileState(addr, desireState string, waitIntervalInSecond int) (dsInfo *api.DataSourceInfo, err error) {
	cli := &client.DataSourceClient{
		Remote: addr,
	}

	endTime := time.Now().Add(time.Duration(waitIntervalInSecond) * time.Second)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for time.Now().Before(endTime) {
		<-ticker.C

		dsInfo, err = cli.Get()
		if err != nil {
			logrus.Warnf("Failed to get the data source info from address %v: %v", addr, err)
			continue
		}

		if dsInfo.State == desireState {
			break
		}

		logrus.Warnf("The data source info from address %v current state %v does not match the desired state %v", addr, dsInfo.State, desireState)
	}

	if dsInfo != nil {
		if dsInfo.State == desireState {
			return dsInfo, nil
		}
		return nil, fmt.Errorf("the data source info from address %v current state %v does not match the desired state %v after waiting for %d second", addr, dsInfo.State, desireState, waitIntervalInSecond)
	}
	return nil, fmt.Errorf("failed to get the data source file info from address %v when waiting for the file become state %v", addr, desireState)
}
