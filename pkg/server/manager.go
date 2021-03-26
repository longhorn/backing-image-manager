package server

import (
	"fmt"
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
	diskUUID                  string
	diskPathOnHost            string
	backingImages             map[string]*BackingImage
	transferringBackingImages map[string]*BackingImage

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
}

func NewManager(diskUUID, diskPathOnHost, portRange string, shutdownCh chan error) (*Manager, error) {
	start, end, err := ParsePortRange(portRange)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		diskUUID:                  diskUUID,
		diskPathOnHost:            diskPathOnHost,
		backingImages:             map[string]*BackingImage{},
		transferringBackingImages: map[string]*BackingImage{},

		portRangeMin:   start,
		portRangeMax:   end,
		availablePorts: util.NewBitmap(start, end),

		lock: &sync.RWMutex{},

		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component":      "backing-image-manager",
				"diskPathOnHost": diskPathOnHost,
			},
		),

		updateCh:   make(chan interface{}),
		shutdownCh: shutdownCh,

		broadcaster: &broadcaster.Broadcaster{},
		broadcastCh: make(chan interface{}),
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
			diskUUID, err := util.GetDiskConfig(types.DiskPath)
			if err != nil {
				m.log.WithError(err).Error("Backing Image Manager: failed to read disk config file before validating backing image files")
			}

			m.lock.RLock()
			if diskUUID != m.diskUUID {
				m.log.Errorf("Backing Image Manager: the disk UUID %v in config file doesn't match the disk UUID %v in backing image manager, will skip validating backing image files then", diskUUID, m.diskUUID)
			} else {
				for _, bi := range m.backingImages {
					if _, err := bi.Get(); err != nil {
						m.log.WithField("backingImage", bi.Name).WithError(err).Error("Backing Image Manager: failed to validate backing image files")
					}
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
	close(m.updateCh)

	for _, bi := range m.backingImages {
		if err := bi.Delete(); err != nil {
			m.log.WithField("backingImage", bi.Name).WithError(err).Error("Backing Image Manager: failed to clean up backing image during shutdown")
		}
	}
}

func (m *Manager) Pull(ctx context.Context, req *rpc.PullRequest) (ret *rpc.BackingImageResponse, err error) {
	log := m.log.WithFields(logrus.Fields{"backingImage": req.Spec.Name, "URL": req.Spec.Url})
	log.Info("Backing Image Manager: prepare to pull backing image")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Backing Image Manager: failed to pull backing image")
		}
	}()

	if req.Spec.Name == "" || req.Spec.Url == "" || req.Spec.Uuid == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	bi := NewBackingImage(req.Spec.Name, req.Spec.Url, req.Spec.Uuid, m.diskPathOnHost)
	if err := m.registerBackingImage(bi); err != nil {
		return nil, err
	}

	biReps, err := bi.Pull()
	if err != nil {
		return nil, err
	}

	log.Info("Backing Image Manager: pulling backing image")
	return biReps, nil
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

func (m *Manager) registerBackingImage(bi *BackingImage) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, exists := m.backingImages[bi.Name]; exists {
		return status.Errorf(codes.AlreadyExists, "Backing Image %v already exists", bi.Name)
	}
	m.backingImages[bi.Name] = bi
	bi.SetUpdateChannel(m.updateCh)
	return nil
}

func (m *Manager) unregisterBackingImage(bi *BackingImage) {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.backingImages, bi.Name)
	delete(m.transferringBackingImages, bi.Name)

	return
}

func (m *Manager) findBackingImage(name string) *BackingImage {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.backingImages[name]
}

func (m *Manager) isTransferringBackingImage(name string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.transferringBackingImages[name] != nil
}

func (m *Manager) Get(ctx context.Context, req *rpc.GetRequest) (*rpc.BackingImageResponse, error) {
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	bi := m.findBackingImage(req.Name)
	if bi == nil {
		return nil, status.Errorf(codes.NotFound, "cannot find backing image %v", req.Name)
	}

	return bi.Get()
}

func (m *Manager) List(ctx context.Context, req *empty.Empty) (*rpc.ListResponse, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	list := map[string]*rpc.BackingImageResponse{}
	for name, bi := range m.backingImages {
		biResp, err := bi.Get()
		if err != nil {
			return nil, err
		}
		list[name] = biResp
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
	if bi := m.findBackingImage(req.BackingImageSpec.Name); bi != nil {
		return nil, status.Errorf(codes.AlreadyExists, "backing image %v already exists in the receiver side", req.BackingImageSpec.Name)
	}

	bi := NewBackingImage(req.BackingImageSpec.Name, req.BackingImageSpec.Url, req.BackingImageSpec.Uuid, m.diskPathOnHost)
	if err := m.registerBackingImage(bi); err != nil {
		return nil, err
	}

	senderManagerAddress := fmt.Sprintf("%s:%d", req.FromHost, types.DefaultPort)
	port, err := bi.Receive(senderManagerAddress, m.allocatePorts, m.releasePorts)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to receive backing image")
	}

	receiverAddress := fmt.Sprintf("%s:%d", req.ToHost, port)
	sender := client.NewBackingImageManagerClient(senderManagerAddress)
	if err = sender.Send(req.BackingImageSpec.Name, receiverAddress); err != nil {
		return nil, errors.Wrapf(err, "sender failed to send backing image")
	}

	log.Info("Backing Image Manager: receiving backing image")
	return bi.Get()
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
	if _, exists := m.transferringBackingImages[req.Name]; exists {
		return nil, status.Errorf(codes.NotFound, "backing image %v is being transferring to a new manager", req.Name)
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

func (m *Manager) OwnershipTransferStart(ctx context.Context, req *empty.Empty) (resp *rpc.OwnershipTransferStartResponse, err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	resp = &rpc.OwnershipTransferStartResponse{
		ReadyBackingImages: map[string]*rpc.BackingImageSpec{},
	}

	m.log.Info("Backing Image Manager: start to transfer files from this old manager")
	for name, bi := range m.backingImages {
		biResp, err := bi.Get()
		if err != nil {
			m.log.WithField("backingImage", bi.Name).WithError(err).Error("Backing Image Manager: failed to get backing image info when upgrade from the old manager")
			continue
		}
		if biResp.Status.SendingReference == 0 && biResp.Status.State == types.DownloadStateDownloaded {
			m.transferringBackingImages[name] = bi
			resp.ReadyBackingImages[name] = biResp.Spec
		}
	}
	m.log.Infof("Backing Image Manager: transferring from this old manager: %+v", m.transferringBackingImages)

	return resp, nil
}

func (m *Manager) OwnershipTransferConfirm(ctx context.Context, req *rpc.OwnershipTransferConfirmRequest) (resp *empty.Empty, err error) {
	defer func() {
		m.updateCh <- nil
	}()
	m.lock.Lock()
	defer m.lock.Unlock()

	// This means the current manager is the old one. This manager need to unregister transferring backing images.
	if len(req.ReadyBackingImages) == 0 {
		m.log.Infof("Backing Image Manager: prepare to unregister transferring backing images from this old manager: %+v", m.transferringBackingImages)
		for name := range m.transferringBackingImages {
			delete(m.backingImages, name)
			delete(m.transferringBackingImages, name)
		}
		m.log.Info("Backing Image Manager: unregistered transferring backing images from this old manager")
		return &empty.Empty{}, nil
	}

	// This means the current manager is the new one and it needs to take over the existing ready backing images
	m.log.Infof("Backing Image Manager: prepare to register transferring backing images to this new manager: %+v", req.ReadyBackingImages)
	for _, biSpec := range req.ReadyBackingImages {
		if _, exists := m.backingImages[biSpec.Name]; exists {
			continue
		}
		bi := IntroduceDownloadedBackingImage(biSpec.Name, biSpec.Url, biSpec.Uuid, m.diskPathOnHost)
		m.backingImages[bi.Name] = bi
		bi.SetUpdateChannel(m.updateCh)
	}
	m.log.Info("Backing Image Manager: registered transferring backing images to this new manager")
	return &empty.Empty{}, nil
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
