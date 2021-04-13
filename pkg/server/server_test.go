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
	//Do nothing for now, just act as the receiving end
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
	c.Assert(err, IsNil)
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

func (s *TestSuite) TestCRUD(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	biw := &BackingImageWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "test_crud_backing_image-" + strconv.Itoa(i)
			go s.m.Watch(nil, biw)

			if i%2 != 0 {
				pullReq := &rpc.PullRequest{
					Spec: &rpc.BackingImageSpec{
						Name: name,
						Url:  "http://" + name + ".com",
						Uuid: name + "-" + "uuid",
						Size: 1024,
					},
				}
				pullResp, err := s.m.Pull(nil, pullReq)
				c.Assert(err, IsNil)
				c.Assert(pullResp.Status.State, Not(Equals), types.DownloadStateFailed)
			} else {
				syncReq := &rpc.SyncRequest{
					BackingImageSpec: &rpc.BackingImageSpec{
						Name: name,
						Url:  "http://" + name + ".com",
						Uuid: name + "-" + "uuid",
						Size: 1024,
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
