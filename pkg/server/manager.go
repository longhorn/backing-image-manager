package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/rpc"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
	"github.com/longhorn/backing-image-manager/pkg/util/broadcaster"
)

/* Lock order
   1. Manager.lock
   2. BackingImage.lock
*/

type Manager struct {
	diskUUID      string
	diskPath      string
	backingImages map[string]*BackingImage

	portRangeMin   int32
	portRangeMax   int32
	availablePorts *util.Bitmap

	// Need to acquire lock when access to BackingImage fields as well as its meta file.
	lock *sync.RWMutex

	log logrus.FieldLogger

	updateCh   chan interface{}
	shutdownCh chan error

	broadcaster *broadcaster.Broadcaster
	broadcastCh chan interface{}

	HandlerFactory HandlerFactory
	Sender         func(string, string, string) error
}

func NewManager(diskUUID, diskPath, portRange string, shutdownCh chan error) (*Manager, error) {
	workDir := filepath.Join(diskPath, types.BackingImageManagerDirectoryName)
	if err := os.Mkdir(workDir, 0666); err != nil && !os.IsExist(err) {
		return nil, err
	}

	start, end, err := ParsePortRange(portRange)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		diskUUID:      diskUUID,
		diskPath:      diskPath,
		backingImages: map[string]*BackingImage{},

		portRangeMin:   start,
		portRangeMax:   end,
		availablePorts: util.NewBitmap(start, end),

		lock: &sync.RWMutex{},

		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component": "backing-image-manager",
			},
		),

		updateCh:   make(chan interface{}),
		shutdownCh: shutdownCh,

		broadcaster: &broadcaster.Broadcaster{},
		broadcastCh: make(chan interface{}),

		// for unit test
		HandlerFactory: &BackingImageHandlerFactory{},
		Sender:         RequestBackingImageSending,
	}

	// help to kickstart the broadcaster
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := m.broadcaster.Subscribe(c, m.broadcastConnector); err != nil {
		return nil, err
	}
	go m.startMonitoring()
	go m.startBroadcasting()

	return m, nil
}

func (m *Manager) startMonitoring() {
	ticker := time.NewTicker(types.FileValidationInterval)
	defer ticker.Stop()

	done := false
	for {
		select {
		case <-m.shutdownCh:
			m.log.Info("Backing Image Manager stops monitoring")
			done = true
			break
		case <-ticker.C:
			diskUUID, err := util.GetDiskConfig(m.diskPath)
			if err != nil {
				m.log.WithError(err).Error("Backing Image Manager: failed to read disk config file before validating backing image files")
				continue
			}

			m.lock.RLock()
			if diskUUID != m.diskUUID {
				m.log.Errorf("Backing Image Manager: the disk UUID %v in config file doesn't match the disk UUID %v in backing image manager, will skip validating backing image files then", diskUUID, m.diskUUID)
			} else {
				for _, bi := range m.backingImages {
					bi.Get()
				}
			}
			m.lock.RUnlock()
		}
		if done {
			break
		}
	}
}

func (m *Manager) startBroadcasting() {
	done := false
	for {
		select {
		case <-m.shutdownCh:
			m.log.Info("Backing Image Manager stops broadcasting")
			done = true
			break
		case <-m.updateCh:
			m.broadcastCh <- nil
		}
		if done {
			break
		}
	}
}

func (m *Manager) Shutdown() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.shutdownCh <- nil
}

func (m *Manager) Delete(ctx context.Context, req *rpc.DeleteRequest) (resp *empty.Empty, err error) {
	log := m.log.WithFields(logrus.Fields{"backingImage": req.Name})
	log.Info("Backing Image Manager: prepare to delete backing image")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Backing Image Manager: failed to delete backing image")
		}
	}()

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}
	bi := m.findBackingImage(req.Name)
	if bi == nil {
		return nil, status.Errorf(codes.NotFound, "cannot find backing image %v", req.Name)
	}

	if err := bi.Delete(); err != nil {
		return nil, err
	}
	m.unregisterBackingImage(bi)

	log.Info("Backing Image Manager: deleted backing image")
	return &empty.Empty{}, nil
}

func (m *Manager) unregisterBackingImage(bi *BackingImage) {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.backingImages, bi.Name)

	return
}

func (m *Manager) findBackingImage(name string) *BackingImage {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.backingImages[name]
}

func (m *Manager) Get(ctx context.Context, req *rpc.GetRequest) (*rpc.BackingImageResponse, error) {
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	bi := m.findBackingImage(req.Name)
	if bi == nil {
		return nil, status.Errorf(codes.NotFound, "cannot find backing image %v", req.Name)
	}

	return bi.Get(), nil
}

func (m *Manager) List(ctx context.Context, req *empty.Empty) (*rpc.ListResponse, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	list := map[string]*rpc.BackingImageResponse{}
	for name, bi := range m.backingImages {
		list[name] = bi.Get()
	}

	return &rpc.ListResponse{BackingImages: list}, nil
}

func (m *Manager) Sync(ctx context.Context, req *rpc.SyncRequest) (resp *rpc.BackingImageResponse, err error) {
	log := m.log.WithFields(logrus.Fields{"backingImage": req.BackingImageSpec.Name, "fromHost": req.FromHost, "toHost": req.ToHost})
	log.Info("Backing Image Manager: prepare to request backing image from others")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Backing Image Manager: failed to start syncing backing image")
		}
	}()

	if req.BackingImageSpec.Name == "" || req.BackingImageSpec.Uuid == "" || req.FromHost == "" || req.ToHost == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	m.lock.Lock()
	bi := m.backingImages[req.BackingImageSpec.Name]
	if bi != nil {
		biResp := bi.Get()
		if biResp.Status.State != string(types.StateFailed) {
			m.lock.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "backing image %v with state %v already exists in the receiver side", req.BackingImageSpec.Name, biResp.Status.State)
		}
		log.Infof("Backing Image Manager: prepare to re-register and re-sync failed backing image")
		delete(m.backingImages, req.BackingImageSpec.Name)
	}
	bi = NewBackingImage(req.BackingImageSpec.Name, req.BackingImageSpec.Uuid, m.diskPath, req.BackingImageSpec.Size, m.HandlerFactory.NewHandler(), m.updateCh)
	m.backingImages[req.BackingImageSpec.Name] = bi
	m.lock.Unlock()

	senderManagerAddress := fmt.Sprintf("%s:%d", req.FromHost, types.DefaultPort)
	port, err := bi.Receive(senderManagerAddress, m.allocatePorts, m.releasePorts)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to receive backing image")
	}

	if port == 0 {
		log.Info("Backing Image Manager: skip syncing backing image")
		return bi.Get(), nil
	}

	receiverAddress := fmt.Sprintf("%s:%d", req.ToHost, port)
	if err = m.Sender(senderManagerAddress, receiverAddress, req.BackingImageSpec.Name); err != nil {
		return nil, errors.Wrapf(err, "sender failed to ask for backing image sending")
	}

	log.Info("Backing Image Manager: receiving backing image")
	return bi.Get(), nil
}

func RequestBackingImageSending(senderAddress, receiverAddress, backingImageName string) error {
	sender := client.NewBackingImageManagerClient(senderAddress)
	return sender.Send(backingImageName, receiverAddress)
}

func (m *Manager) Send(ctx context.Context, req *rpc.SendRequest) (resp *empty.Empty, err error) {
	log := m.log.WithFields(logrus.Fields{"backingImage": req.Name, "toAddress": req.ToAddress})
	log.Info("Backing Image Manager: prepare to send backing image")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Backing Image Manager: failed to start sending backing image")
		}
	}()

	if req.Name == "" || req.ToAddress == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	bi, exists := m.backingImages[req.Name]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "backing image %v not found in the send side", req.Name)
	}

	if err := bi.Send(req.ToAddress, m.allocatePorts, m.releasePorts); err != nil {
		return nil, err
	}

	log.Infof("Backing Image Manager: sending backing image")
	return &empty.Empty{}, nil
}

func (m *Manager) allocatePorts(portCount int32) (int32, int32, error) {
	if portCount < 0 {
		return 0, 0, fmt.Errorf("invalid port count %v", portCount)
	}
	if portCount == 0 {
		return 0, 0, nil
	}
	start, end, err := m.availablePorts.AllocateRange(portCount)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "fail to allocate %v ports", portCount)
	}
	return start, end, nil
}

func (m *Manager) releasePorts(start, end int32) error {
	if start < 0 || end < 0 {
		return fmt.Errorf("invalid start/end port %v %v", start, end)
	}
	return m.availablePorts.ReleaseRange(start, end)
}

func ParsePortRange(portRange string) (int32, int32, error) {
	if portRange == "" {
		return 0, 0, fmt.Errorf("Empty port range")
	}
	parts := strings.Split(portRange, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("Invalid format for range: %s", portRange)
	}
	portStart, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("Invalid start port for range: %s", err)
	}
	portEnd, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("Invalid end port for range: %s", err)
	}
	return int32(portStart), int32(portEnd), nil
}

func (m *Manager) Watch(req *empty.Empty, srv rpc.BackingImageManagerService_WatchServer) (err error) {
	m.log.Info("Backing Image Manager: prepare to start backing image update watch")

	responseChan, err := m.Subscribe()
	if err != nil {
		m.log.WithError(err).Error("Backing Image Manager: failed to subscribe response channel")
		return err
	}

	defer func() {
		if err != nil {
			m.log.WithError(err).Error("Backing Image Manager: backing image update watch errored out")
		} else {
			m.log.Info("Backing Image Manager: backing image update watch ended successfully")
		}
	}()
	m.log.Info("Backing Image Manager: backing image update watch started")

	for range responseChan {
		if err := srv.Send(&empty.Empty{}); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) broadcastConnector() (chan interface{}, error) {
	return m.broadcastCh, nil
}

func (m *Manager) Subscribe() (<-chan interface{}, error) {
	return m.broadcaster.Subscribe(context.TODO(), m.broadcastConnector)
}
