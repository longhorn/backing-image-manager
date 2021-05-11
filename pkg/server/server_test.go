package server

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/longhorn/backing-image-manager/pkg/rpc"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
	"github.com/longhorn/backing-image-manager/pkg/util/uploadserver"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	shutdownCh chan error
	m          *Manager
}

var _ = Suite(&TestSuite{})

type BackingImageWatcher struct {
	grpc.ServerStream
}

func (biw *BackingImageWatcher) Send(empty *empty.Empty) error {
	// Do nothing for now, just act as the receiving end
	return nil
}

func MockSender(senderAddress, receiverAddress, backingImageName string) error {
	return nil
}

func (s *TestSuite) getTestDiskPath(c *C) string {
	currentUser, err := user.Current()
	c.Assert(err, IsNil)
	return currentUser.HomeDir
}

func (s *TestSuite) SetUpSuite(c *C) {
	var err error

	logrus.SetLevel(logrus.DebugLevel)

	testDiskPath := s.getTestDiskPath(c)
	if _, err := os.Stat(testDiskPath); os.IsNotExist(err) {
		err = os.Mkdir(filepath.Join(testDiskPath, types.BackingImageManagerDirectoryName), 0777)
		c.Assert(err, IsNil)
	} else {
		c.Assert(err, IsNil)
	}

	diskCfgPath := filepath.Join(testDiskPath, util.DiskConfigFile)
	if _, err := os.Stat(diskCfgPath); os.IsNotExist(err) {
		diskCfg := &util.DiskConfig{
			DiskUUID: "bim-test-disk-cfg",
		}
		encodedDiskCfg, err := json.Marshal(diskCfg)
		c.Assert(err, IsNil)
		err = ioutil.WriteFile(diskCfgPath, encodedDiskCfg, 0777)
		c.Assert(err, IsNil)
	} else {
		c.Assert(err, IsNil)
	}

	s.shutdownCh = make(chan error)
	s.m, err = NewManager("bim-test-disk-cfg", "/var/lib/longhorn", testDiskPath, "30001-31000", s.shutdownCh)
	if err != nil {
		c.Assert(os.IsExist(err), Equals, true)
	}
	s.m.DownloaderFactory = &MockDownloaderFactory{}
	s.m.Sender = MockSender
}

func (s *TestSuite) TearDownSuite(c *C) {
	testDiskPath := s.getTestDiskPath(c)
	diskCfgPath := filepath.Join(testDiskPath, util.DiskConfigFile)
	os.RemoveAll(diskCfgPath)
	os.RemoveAll(filepath.Join(testDiskPath, types.BackingImageManagerDirectoryName))
	close(s.shutdownCh)
}

func (s *TestSuite) TestManagerCRUD(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	biw := &BackingImageWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "test_manager_crud_backing_image-" + strconv.Itoa(i)
			go s.m.Watch(nil, biw)

			if i%2 != 0 {
				pullReq := &rpc.PullRequest{
					Spec: &rpc.BackingImageSpec{
						Name: name,
						Url:  "https://" + name + ".com",
						Uuid: name + "-" + "uuid",
					},
				}
				pullResp, err := s.m.Pull(nil, pullReq)
				c.Assert(err, IsNil)
				c.Assert(pullResp.Status.State, Not(Equals), types.DownloadStateFailed)
			} else {
				syncReq := &rpc.SyncRequest{
					BackingImageSpec: &rpc.BackingImageSpec{
						Name: name,
						Url:  "https://" + name + ".com",
						Uuid: name + "-" + "uuid",
						Size: MockDownloadSize,
					},
					FromHost: "from-host-" + strconv.Itoa(i/2),
					ToHost:   "to-host-" + strconv.Itoa(i/2),
				}
				syncResp, err := s.m.Sync(nil, syncReq)
				c.Assert(err, IsNil)
				c.Assert(syncResp.Status.State, Not(Equals), types.DownloadStateFailed)
			}

			getResp, err := s.m.Get(nil, &rpc.GetRequest{
				Name: name,
			})
			c.Assert(err, IsNil)
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Status.State, Not(Equals), types.DownloadStateFailed)

			listResp, err := s.m.List(nil, &empty.Empty{})
			c.Assert(err, IsNil)
			c.Assert(listResp.BackingImages[name], NotNil)
			c.Assert(listResp.BackingImages[name].Spec.Name, Equals, name)
			c.Assert(listResp.BackingImages[name].Status.State, Not(Equals), types.DownloadStateFailed)

			downloaded := false
			for j := 0; j < RetryCount; j++ {
				getResp, err := s.m.Get(nil, &rpc.GetRequest{
					Name: name,
				})
				c.Assert(err, IsNil)
				if getResp.Status.State == types.DownloadStateDownloaded {
					downloaded = true
					break
				}
				time.Sleep(RetryInterval)
			}
			c.Assert(downloaded, Equals, true)

			deleteReq := &rpc.DeleteRequest{
				Name: name,
			}
			_, err = s.m.Delete(nil, deleteReq)
			c.Assert(err, IsNil)
			getResp, err = s.m.Get(nil, &rpc.GetRequest{
				Name: name,
			})
			c.Assert(err, NotNil)
			c.Assert(status.Code(err), Equals, codes.NotFound)
		}(i)
	}
	wg.Wait()
}

func (s *TestSuite) TestSingleBackingImageCRUD(c *C) {
	name := "test_crud_backing_image"
	url := "https://" + name + ".com"
	uuid := name + "-" + "uuid"

	mockDownloaderFactory := &MockDownloaderFactory{}

	// Each iteration takes 5 seconds.
	count := 10
	for i := 0; i < count; i++ {
		bi := NewBackingImage(name, url, uuid, "/var/lib/longhorn", s.getTestDiskPath(c), mockDownloaderFactory.NewDownloader(), make(chan interface{}, 100))
		var err error

		err = bi.Delete()
		c.Assert(err, IsNil)

		if i%2 != 0 {
			_, err = bi.Pull()
		} else {
			_, err = bi.Receive(MockDownloadSize, "SenderAddress", func(portCount int32) (int32, int32, error) {
				return 0, 0, nil
			}, func(start, end int32) error {
				return nil
			})
		}
		c.Assert(err, IsNil)

		downloaded := false
		for j := 0; j < RetryCount; j++ {
			getResp := bi.Get()
			if getResp.Status.State == types.DownloadStateDownloaded {
				downloaded = true
				break
			}
			time.Sleep(RetryInterval)
		}
		c.Assert(downloaded, Equals, true)

		err = bi.Delete()
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestBackingImageSimultaneousDownloadingAndCancellation(c *C) {
	name := "test_simultaneous_downloading_and_cancellation_backing_image"
	url := "https://" + name + ".com"
	uuid := name + "-" + "uuid"

	mockDownloaderFactory := &MockDownloaderFactory{}

	count := 100
	for i := 0; i < count; i++ {
		bi := NewBackingImage(name, url, uuid, "/var/lib/longhorn", s.getTestDiskPath(c), mockDownloaderFactory.NewDownloader(), make(chan interface{}, 100))

		err := bi.Delete()
		c.Assert(err, IsNil)

		// Start to do Pulling and Receiving simultaneously, which is impossible ideally.
		// Then there must an pull error or sync error.
		var errPull, errSync error
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, errPull = bi.Pull()
		}()
		go func() {
			defer wg.Done()
			_, errSync = bi.Receive(MockDownloadSize, "SenderAddress", func(portCount int32) (int32, int32, error) {
				return 0, 0, nil
			}, func(start, end int32) error {
				return nil
			})
		}()
		wg.Wait()
		c.Assert((errPull == nil && errSync != nil) || (errPull != nil && errSync == nil), Equals, true)

		isDownloading := false
		for j := 0; j < 5; j++ {
			getResp := bi.Get()
			if getResp.Status.State == types.DownloadStateDownloading && getResp.Status.DownloadProgress > 0 {
				isDownloading = true
				break
			}
			time.Sleep(RetryInterval)
		}
		c.Assert(isDownloading, Equals, true)
		err = bi.Delete()
		c.Assert(err, IsNil)
		getResp := bi.Get()
		c.Assert(getResp.Status.State, Equals, string(StateFailed))
	}
}

func (s *TestSuite) TestUpload(c *C) {
	dir := s.getTestDiskPath(c)
	originalFilePath := filepath.Join(dir, "original")
	err := util.GenerateRandomDataFile(originalFilePath, 100)
	c.Assert(err, IsNil)

	count := 10
	wg := &sync.WaitGroup{}
	biw := &BackingImageWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		f, err := os.OpenFile(originalFilePath, os.O_RDONLY, 0666)
		c.Assert(err, IsNil)

		stat, err := f.Stat()
		c.Assert(err, IsNil)
		fileSize := stat.Size()

		go func(i int) {
			defer wg.Done()
			name := "test_upload-" + strconv.Itoa(i)
			uuid := name + "-" + "uuid"
			go s.m.Watch(nil, biw)

			deleteReq := &rpc.DeleteRequest{
				Name: name,
			}
			_, err = s.m.Delete(nil, deleteReq)

			uploadReq := &rpc.UploadServerLaunchRequest{
				Spec: &rpc.BackingImageSpec{
					Name: name,
					Uuid: uuid,
				},
			}
			uploadResp, err := s.m.UploadServerLaunch(nil, uploadReq)
			c.Assert(err, IsNil)
			c.Assert(uploadResp.Status.UploadPort, Not(Equals), 0)

			getResp, err := s.m.Get(nil, &rpc.GetRequest{
				Name: name,
			})
			c.Assert(err, IsNil)
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Status.State, Equals, types.DownloadStateStarting)

			cli := uploadserver.UploadClient{
				Remote:    "localhost:" + strconv.Itoa(int(uploadResp.Status.UploadPort)),
				Directory: dir,
			}

			// Wait for server starting
			for retry := 0; retry < 5; retry++ {
				if err = cli.Start(fileSize); err == nil {
					break
				}
				time.Sleep(RetryInterval)
			}
			c.Assert(err, IsNil)

			offset := int64(0)
			chunkCount := int64(0)
			for index := 0; offset < fileSize; index++ {
				// Upload 2Mi ~ 4Mi data in each HTTP Post
				chunkSize := rand.Int63n(2*1024*1024) + 2*1024*1024
				if fileSize-offset <= chunkSize {
					chunkSize = fileSize - offset
				}
				data := make([]byte, chunkSize)
				_, err = f.ReadAt(data, offset)
				c.Assert(err, IsNil)
				offset += chunkSize

				exists, err := cli.PrepareChunk(index, data)
				c.Assert(err, IsNil)
				c.Assert(exists, Equals, false)

				err = cli.UploadChunk(index, data)
				c.Assert(err, IsNil)
				chunkCount++

				getResp, err = s.m.Get(nil, &rpc.GetRequest{
					Name: name,
				})
				c.Assert(err, IsNil)
				c.Assert(getResp.Spec.Name, Equals, name)
				c.Assert(getResp.Status.State, Equals, types.DownloadStateDownloading)
				c.Assert(getResp.Status.DownloadProgress, Not(Equals), 0)
			}

			err = cli.CoalesceChunk(stat.Size(), chunkCount)
			c.Assert(err, IsNil)

			cli.Close()
			err = f.Close()
			c.Assert(err, IsNil)

			for retry := 0; retry < 5; retry++ {
				getResp, err = s.m.Get(nil, &rpc.GetRequest{
					Name: name,
				})
				c.Assert(err, IsNil)
				if getResp.Status.State == types.DownloadStateDownloaded && getResp.Status.DownloadProgress == 100 {
					break
				}
				time.Sleep(RetryInterval)
			}

			listResp, err := s.m.List(nil, &empty.Empty{})
			c.Assert(err, IsNil)
			c.Assert(listResp.BackingImages[name], NotNil)
			c.Assert(listResp.BackingImages[name].Spec.Name, Equals, name)
			c.Assert(listResp.BackingImages[name].Status.State, Equals, types.DownloadStateDownloaded)

			uploadedFilePath := filepath.Join(dir, types.BackingImageManagerDirectoryName,
				GetBackingImageDirectoryName(name, uuid), types.BackingImageFileName)
			err = exec.Command("diff", originalFilePath, uploadedFilePath).Run()
			c.Assert(err, IsNil)

			deleteReq = &rpc.DeleteRequest{
				Name: name,
			}
			_, err = s.m.Delete(nil, deleteReq)
			c.Assert(err, IsNil)
			getResp, err = s.m.Get(nil, &rpc.GetRequest{
				Name: name,
			})
			c.Assert(err, NotNil)
			c.Assert(status.Code(err), Equals, codes.NotFound)
		}(i)
	}
	wg.Wait()
}
