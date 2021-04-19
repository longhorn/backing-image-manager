package server

import (
	"encoding/json"
	"io/ioutil"
	"os"
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

	s.m, err = NewManager("", "/var/lib/longhorn", testDiskPath, "30001-31000", make(chan error))
	if err != nil {
		c.Assert(os.IsExist(err), Equals, true)
	}
	s.m.DownloaderFactory = &MockDownloaderFactory{}
	s.shutdownCh = make(chan error)
	s.m.diskPathInContainer = testDiskPath
	s.m.diskUUID = "bim-test-disk-cfg"
	s.m.Sender = MockSender
}

func (s *TestSuite) TearDownSuite(c *C) {
	testDiskPath := s.getTestDiskPath(c)
	diskCfgPath := filepath.Join(testDiskPath, util.DiskConfigFile)
	os.RemoveAll(diskCfgPath)
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
		bm := NewBackingImage(name, url, uuid, "/var/lib/longhorn", s.getTestDiskPath(c), mockDownloaderFactory.NewDownloader(), make(chan interface{}, 100))
		var err error

		err = bm.Delete()
		c.Assert(err, IsNil)

		if i%2 != 0 {
			_, err = bm.Pull()
		} else {
			_, err = bm.Receive(MockDownloadSize, "SenderAddress", func(portCount int32) (int32, int32, error) {
				return 0, 0, nil
			}, func(start, end int32) error {
				return nil
			})
		}
		c.Assert(err, IsNil)

		downloaded := false
		for j := 0; j < RetryCount; j++ {
			getResp := bm.Get()
			if getResp.Status.State == types.DownloadStateDownloaded {
				downloaded = true
				break
			}
			time.Sleep(RetryInterval)
		}
		c.Assert(downloaded, Equals, true)

		err = bm.Delete()
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
		bm := NewBackingImage(name, url, uuid, "/var/lib/longhorn", s.getTestDiskPath(c), mockDownloaderFactory.NewDownloader(), make(chan interface{}, 100))

		err := bm.Delete()
		c.Assert(err, IsNil)

		// Start to do Pulling and Receiving simultaneously, which is impossible ideally.
		// Then there must an pull error or sync error.
		var errPull, errSync error
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, errPull = bm.Pull()
		}()
		go func() {
			defer wg.Done()
			_, errSync = bm.Receive(MockDownloadSize, "SenderAddress", func(portCount int32) (int32, int32, error) {
				return 0, 0, nil
			}, func(start, end int32) error {
				return nil
			})
		}()
		wg.Wait()
		c.Assert((errPull == nil && errSync != nil) || (errPull != nil && errSync == nil), Equals, true)

		isDownloading := false
		for j := 0; j < 5; j++ {
			getResp := bm.Get()
			if getResp.Status.State == types.DownloadStateDownloading && getResp.Status.DownloadProgress > 0 {
				isDownloading = true
				break
			}
			time.Sleep(RetryInterval)
		}
		c.Assert(isDownloading, Equals, true)
		err = bm.Delete()
		c.Assert(err, IsNil)
		getResp := bm.Get()
		c.Assert(getResp.Status.State, Equals, string(StateFailed))
	}
}
