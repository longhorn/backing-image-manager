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

	s.shutdownCh = make(chan error)
	s.m, err = NewManager("bim-test-disk-cfg", testDiskPath, "30001-31000", s.shutdownCh)
	if err != nil {
		c.Assert(os.IsExist(err), Equals, true)
	}
	s.m.HandlerFactory = &MockHandlerFactory{}
	s.m.Sender = MockSender
}

func (s *TestSuite) TearDownSuite(c *C) {
	testDiskPath := s.getTestDiskPath(c)
	diskCfgPath := filepath.Join(testDiskPath, util.DiskConfigFile)
	os.RemoveAll(diskCfgPath)
	close(s.shutdownCh)
}

func (s *TestSuite) TestManagerSync(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	biw := &BackingImageWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "test_manager_sync_backing_image-" + strconv.Itoa(i)
			go s.m.Watch(nil, biw)

			syncReq := &rpc.SyncRequest{
				BackingImageSpec: &rpc.BackingImageSpec{
					Name: name,
					Uuid: name + "-" + "uuid",
					Size: MockProcessingSize,
				},
				FromHost: "from-host-" + strconv.Itoa(i),
				ToHost:   "to-host-" + strconv.Itoa(i),
			}
			syncResp, err := s.m.Sync(nil, syncReq)
			c.Assert(err, IsNil)
			c.Assert(syncResp.Status.State, Not(Equals), types.StateFailed)

			getResp, err := s.m.Get(nil, &rpc.GetRequest{
				Name: name,
			})
			c.Assert(err, IsNil)
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Status.State, Not(Equals), types.StateFailed)

			listResp, err := s.m.List(nil, &empty.Empty{})
			c.Assert(err, IsNil)
			c.Assert(listResp.BackingImages[name], NotNil)
			c.Assert(listResp.BackingImages[name].Spec.Name, Equals, name)
			c.Assert(listResp.BackingImages[name].Status.State, Not(Equals), types.StateFailed)

			isReady := false
			for j := 0; j < RetryCount; j++ {
				getResp, err := s.m.Get(nil, &rpc.GetRequest{
					Name: name,
				})
				c.Assert(err, IsNil)
				if getResp.Status.State == string(types.StateReady) {
					isReady = true
					break
				}
				time.Sleep(RetryInterval)
			}
			c.Assert(isReady, Equals, true)

			deleteReq := &rpc.DeleteRequest{
				Name: name,
			}
			_, err = s.m.Delete(nil, deleteReq)
			c.Assert(err, IsNil)
			_, err = s.m.Get(nil, &rpc.GetRequest{
				Name: name,
			})
			c.Assert(err, NotNil)
			c.Assert(status.Code(err), Equals, codes.NotFound)
		}(i)
	}
	wg.Wait()
}

func (s *TestSuite) TestSingleBackingImageSync(c *C) {
	name := "test_backing_image_sync"
	uuid := name + "-" + "uuid"

	mockHandlerFactory := &MockHandlerFactory{}

	// Each iteration takes around 5 seconds.
	count := 10
	for i := 0; i < count; i++ {
		updateCh := make(chan interface{}, 200)
		bi := NewBackingImage(name, uuid, s.getTestDiskPath(c), MockProcessingSize, mockHandlerFactory.NewHandler(), updateCh)
		var err error

		err = bi.Delete()
		c.Assert(err, IsNil)

		_, err = bi.Receive("SenderAddress", func(portCount int32) (int32, int32, error) {
			return 0, 0, nil
		}, func(start, end int32) error {
			return nil
		})
		c.Assert(err, IsNil)

		isReady := false
		for j := 0; j < RetryCount; j++ {
			getResp := bi.Get()
			if getResp.Status.State == string(types.StateReady) {
				isReady = true
				break
			}
			time.Sleep(RetryInterval)
		}
		c.Assert(isReady, Equals, true)

		err = bi.Delete()
		c.Assert(err, IsNil)
		close(updateCh)
	}
}

func (s *TestSuite) TestBackingImageSimultaneousProcessingAndCancellation(c *C) {
	name := "test_simultaneous_processing_and_cancellation_backing_image"
	uuid := name + "-" + "uuid"

	mockHandlerFactory := &MockHandlerFactory{}

	count := 100
	for i := 0; i < count; i++ {
		bi := NewBackingImage(name, uuid, s.getTestDiskPath(c), MockProcessingSize, mockHandlerFactory.NewHandler(), make(chan interface{}, 100))

		err := bi.Delete()
		c.Assert(err, IsNil)

		// Start to do 2 Receivers simultaneously, which is impossible ideally.
		// Then there must an error.
		var errSync1, errSync2 error
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, errSync1 = bi.Receive("SenderAddress1", func(portCount int32) (int32, int32, error) {
				return 0, 0, nil
			}, func(start, end int32) error {
				return nil
			})
		}()
		go func() {
			defer wg.Done()
			_, errSync2 = bi.Receive("SenderAddress2", func(portCount int32) (int32, int32, error) {
				return 0, 0, nil
			}, func(start, end int32) error {
				return nil
			})
		}()
		wg.Wait()
		c.Assert((errSync1 == nil && errSync2 != nil) || (errSync1 != nil && errSync2 == nil), Equals, true)

		isProcessing := false
		for j := 0; j < 5; j++ {
			getResp := bi.Get()
			if getResp.Status.State == string(types.StateInProgress) && getResp.Status.Progress > 0 {
				isProcessing = true
				break
			}
			time.Sleep(RetryInterval)
		}
		c.Assert(isProcessing, Equals, true)
		err = bi.Delete()
		c.Assert(err, IsNil)
		getResp := bi.Get()
		c.Assert(getResp.Status.State, Equals, string(types.StateFailed))
	}
}
