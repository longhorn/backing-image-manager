package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/rpc"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
	"github.com/longhorn/backing-image-manager/pkg/util/broadcaster"
)

type Manager struct {
	ctx context.Context

	syncAddress  string
	diskUUID     string
	diskPath     string
	portRangeMin int32

	portRangeMax   int32
	availablePorts *util.Bitmap

	// Need to acquire lock when operating biFileInfoMap or broadcastRequired.
	lock          *sync.RWMutex
	biFileInfoMap map[string]*api.FileInfo

	broadcastRequired bool
	broadcastCh       chan interface{}
	broadcaster       *broadcaster.Broadcaster

	syncClient *client.SyncClient

	log logrus.FieldLogger
}

func NewManager(ctx context.Context, syncAddress, diskUUID, diskPath, portRange string) (*Manager, error) {
	workDir := filepath.Join(diskPath, types.BackingImageManagerDirectoryName)
	if err := os.MkdirAll(workDir, 0666); err != nil && !os.IsExist(err) {
		return nil, err
	}

	start, end, err := ParsePortRange(portRange)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		ctx: ctx,

		syncAddress: syncAddress,
		diskUUID:    diskUUID,
		diskPath:    diskPath,

		portRangeMin:   start,
		portRangeMax:   end,
		availablePorts: util.NewBitmap(start, end),

		lock:          &sync.RWMutex{},
		biFileInfoMap: map[string]*api.FileInfo{},

		broadcaster: &broadcaster.Broadcaster{},
		broadcastCh: make(chan interface{}),

		syncClient: &client.SyncClient{
			Remote: syncAddress,
		},

		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component": "backing-image-manager",
				"diskPath":  diskPath,
				"diskUUID":  diskUUID,
			},
		),
	}

	// help to kickstart the broadcaster
	if _, err := m.broadcaster.Subscribe(ctx, m.broadcastConnector); err != nil {
		return nil, err
	}
	go m.monitoring()
	go m.startBroadcasting(ctx)

	return m, nil
}

func (m *Manager) startBroadcasting(ctx context.Context) {
	ticker := time.NewTicker(types.MonitorInterval)
	defer ticker.Stop()

	done := false
	for !done {
		select {
		case <-ctx.Done():
			m.log.Info("Backing Image Manager: stopped broadcasting due to context done")
			done = true
		case <-ticker.C:
			if m.checkBroadcasting() {
				m.broadcastCh <- nil
			}
		}
	}
}

func (m *Manager) checkBroadcasting() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.broadcastRequired {
		m.broadcastRequired = false
		return true
	}
	return false
}

func (m *Manager) monitoring() {
	ticker := time.NewTicker(types.MonitorInterval)
	defer ticker.Stop()

	done := false
	for {
		select {
		case <-m.ctx.Done():
			m.log.Info("Backing Image Manager: stopped monitoring due to the context done")
			done = true
			break
		case <-ticker.C:
			m.listAndUpdate()
		}
		if done {
			break
		}
	}
}

func (m *Manager) Delete(ctx context.Context, req *rpc.DeleteRequest) (resp *empty.Empty, err error) {
	log := m.log.WithFields(logrus.Fields{"biName": req.Name, "biUUID": req.Uuid})
	log.Info("Backing Image Manager: prepare to delete backing image")
	defer func() {
		if err != nil {
			log.WithError(err).Warn("Backing Image Manager: failed to delete backing image, will continue to do directory cleanup anyway")
		}
		if rmDirErr := os.RemoveAll(types.GetBackingImageDirectory(m.diskPath, req.Name, req.Uuid)); rmDirErr != nil {
			log.WithError(rmDirErr).Warn("Backing Image Manager: failed to remove the backing image work directory at the end of the deletion")
		}
		// Delete cmd is used to remove the tmp file left on the host as well when preparing backing image file failed.
		tmpFilePath := fmt.Sprintf("%s%s", types.GetDataSourceFilePath(m.diskPath, req.Name, req.Uuid), types.TmpFileSuffix)
		if rmTmpFileErr := os.RemoveAll(tmpFilePath); rmTmpFileErr != nil {
			log.WithError(rmTmpFileErr).Warn("Backing Image Manager: failed to remove the data source tmp file at the end of the deletion")
		}
	}()

	if err := m.syncClient.Delete(types.GetBackingImageFilePath(m.diskPath, req.Name, req.Uuid)); err != nil {
		return nil, err
	}

	if err := m.waitForFileDeleted(req.Name, req.Uuid, 3); err != nil {
		return nil, err
	}

	log.Info("Backing Image Manager: deleted backing image")
	return &empty.Empty{}, nil
}

func (m *Manager) waitForFileDeleted(name, uuid string, waitIntervalInSecond int) (err error) {
	endTime := time.Now().Add(time.Duration(waitIntervalInSecond) * time.Second)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for time.Now().Before(endTime) {
		<-ticker.C
		if _, err := m.getAndUpdate(name, uuid); util.IsGRPCErrorNotFound(err) {
			return nil
		}
	}
	return fmt.Errorf("failed to wait for the backing image %v(%v) deleted within %v second", name, uuid, waitIntervalInSecond)
}

func (m *Manager) Get(ctx context.Context, req *rpc.GetRequest) (*rpc.BackingImageResponse, error) {
	return m.getAndUpdate(req.Name, req.Uuid)
}

func (m *Manager) getAndUpdate(name, uuid string) (*rpc.BackingImageResponse, error) {
	if name == "" || uuid == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	fInfo, err := m.syncClient.Get(types.GetBackingImageFilePath(m.diskPath, name, uuid))
	if err != nil {
		if util.IsHTTPClientErrorNotFound(err) {
			m.lock.Lock()
			if _, exists := m.biFileInfoMap[name]; exists {
				delete(m.biFileInfoMap, name)
				m.broadcastRequired = true
			}
			m.lock.Unlock()
			return nil, status.Errorf(codes.NotFound, "cannot find backing image %v(%v)", name, uuid)
		}
		return nil, err
	}

	m.lock.Lock()
	if !reflect.DeepEqual(m.biFileInfoMap[name], fInfo) {
		m.biFileInfoMap[name] = fInfo
		m.broadcastRequired = true
	}
	m.lock.Unlock()

	return backingImageResponse(fInfo), nil
}

func (m *Manager) List(ctx context.Context, req *empty.Empty) (*rpc.ListResponse, error) {
	biFileInfoMap, err := m.listAndUpdate()
	if err != nil {
		return nil, err
	}

	biMap := map[string]*rpc.BackingImageResponse{}
	for biName, fInfo := range biFileInfoMap {
		biMap[biName] = backingImageResponse(fInfo)
	}

	return &rpc.ListResponse{BackingImages: biMap}, nil
}

func (m *Manager) listAndUpdate() (biFileInfoMap map[string]*api.FileInfo, err error) {
	defer func() {
		if err != nil {
			m.log.WithError(err).Errorf("Backing Image Manager: failed to list and update backing image backing image files")
		}
	}()

	fInfoList, err := m.syncClient.List()
	if err != nil {
		return nil, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	newBiFileInfoMap := map[string]*api.FileInfo{}
	for filePath, fInfo := range fInfoList {
		biName := types.GetBackingImageNameFromFilePath(filePath, fInfo.UUID)
		newBiFileInfoMap[biName] = fInfo
		if !m.broadcastRequired && !reflect.DeepEqual(m.biFileInfoMap[biName], fInfo) {
			m.broadcastRequired = true
		}
	}
	for biName := range m.biFileInfoMap {
		if newBiFileInfoMap[biName] == nil {
			m.broadcastRequired = true
		}
		if m.broadcastRequired {
			break
		}
	}
	m.biFileInfoMap = newBiFileInfoMap

	copiedMap := map[string]*api.FileInfo{}
	for biName, fInfo := range m.biFileInfoMap {
		copiedInfo := *fInfo
		copiedMap[biName] = &copiedInfo
	}
	return copiedMap, nil
}

func backingImageResponse(fInfo *api.FileInfo) *rpc.BackingImageResponse {
	return &rpc.BackingImageResponse{
		Spec: &rpc.BackingImageSpec{
			Name:     types.GetBackingImageNameFromFilePath(fInfo.FilePath, fInfo.UUID),
			Uuid:     fInfo.UUID,
			Size:     fInfo.Size,
			Checksum: fInfo.ExpectedChecksum,
		},
		Status: &rpc.BackingImageStatus{
			State:            fInfo.State,
			Checksum:         fInfo.CurrentChecksum,
			Progress:         int32(fInfo.Progress),
			ErrorMsg:         fInfo.Message,
			SendingReference: int32(fInfo.SendingReference),
		},
	}
}

func (m *Manager) Sync(ctx context.Context, req *rpc.SyncRequest) (resp *rpc.BackingImageResponse, err error) {
	log := m.log.WithFields(logrus.Fields{"biName": req.Spec.Name, "biUUID": req.Spec.Uuid, "fromAddress": req.FromAddress})
	log.Info("Backing Image Manager: prepare to sync backing image")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Backing Image Manager: failed to start receiving backing image")
		}
	}()

	port, _, err := m.allocatePorts(1)
	if err != nil {
		return nil, err
	}
	portReleaseChannel := make(chan interface{}, 1)
	go func() {
		<-portReleaseChannel
		log.Infof("Backing Image Manager: start to release port %v after syncing", port)
		if !util.DetectHTTPServerAvailability(fmt.Sprintf("http://localhost:%d", port), 180, false) {
			log.Errorf("Backing Image Manager: failed to wait for the HTTP server using port %v stopped, cannot release the port", port)
			return
		}
		if err := m.releasePorts(port, port+1); err != nil {
			log.WithError(err).Errorf("Backing Image Manager: failed to release port %v after syncing backing image", port)
		}
		log.Infof("Backing Image Manager: released port %v after syncing", port)
	}()

	biFilePath := types.GetBackingImageFilePath(m.diskPath, req.Spec.Name, req.Spec.Uuid)
	if err := m.syncClient.Receive(biFilePath, req.Spec.Uuid, m.diskUUID, req.Spec.Checksum, "", int(port), req.Spec.Size); err != nil {
		portReleaseChannel <- nil
		return nil, err
	}

	go func() {
		var biResp *rpc.BackingImageResponse

		defer func() {
			if err != nil {
				portReleaseChannel <- nil
				log.WithError(err).Error("Backing Image Manager: failed to request sending the backing image")
				return
			}

			// Wait for 24 hours
			if biResp, err = m.waitForEndingFileState(req.Spec.Name, req.Spec.Uuid, 24*60*60); err != nil {
				log.WithError(err).Errorf("Backing Image Manager: timeout waiting before releasing port %v", port)
			}
			portReleaseChannel <- nil
		}()

		if biResp, err = m.waitForFileStateNonPending(req.Spec.Name, req.Spec.Uuid, 300); err != nil {
			return
		}
		if biResp.Status.State != string(types.StateStarting) {
			err = fmt.Errorf("there is no need to request backing image since the current state is %v rather than %v", biResp.Status.State, types.StateStarting)
			return
		}

		toIP, err := util.GetIPForPod()
		if err != nil {
			return
		}
		toAddress := fmt.Sprintf("%s:%d", toIP, port)

		// sender.Send is a non-blocking call
		sender := client.NewBackingImageManagerClient(req.FromAddress)
		if err = sender.Send(req.Spec.Name, req.Spec.Uuid, toAddress); err != nil {
			err = errors.Wrapf(err, "sender failed to request backing image sending to %v", toAddress)
			return
		}

		log.Infof("Backing Image Manager: started requesting sending backing image from address %v to address %v", req.FromAddress, toAddress)
	}()

	log.Infof("Backing Image Manager: started receiving backing image at port %v", port)

	return m.getAndUpdate(req.Spec.Name, req.Spec.Uuid)
}

func (m *Manager) waitForFileStateNonPending(name, uuid string, waitInterval int) (biResp *rpc.BackingImageResponse, err error) {
	endTime := time.Now().Add(time.Duration(waitInterval) * time.Second)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for time.Now().Before(endTime) {
		<-ticker.C
		if biResp, _ = m.getAndUpdate(name, uuid); biResp != nil && biResp.Status.State != string(types.StatePending) {
			return biResp, nil
		}
	}

	return nil, fmt.Errorf("failed to wait for backing image %v(%v) becoming state non-pending", name, uuid)
}

func (m *Manager) waitForEndingFileState(name, uuid string, waitInterval int) (biResp *rpc.BackingImageResponse, err error) {
	endTime := time.Now().Add(time.Duration(waitInterval) * time.Second)

	ticker := time.NewTicker(types.MonitorInterval)
	defer ticker.Stop()
	for time.Now().Before(endTime) {
		<-ticker.C
		biResp, err = m.getAndUpdate(name, uuid)
		if util.IsGRPCErrorNotFound(err) ||
			(biResp != nil && (biResp.Status.State == string(types.StateReady) || biResp.Status.State == string(types.StateFailed))) {
			return biResp, nil
		}
	}

	state := "unavailable"
	if biResp != nil {
		state = biResp.Status.State
	}
	return nil, fmt.Errorf("failed to wait for backing image %v(%v) becoming an ending state, current state %v", name, uuid, state)
}

func (m *Manager) Send(ctx context.Context, req *rpc.SendRequest) (resp *empty.Empty, err error) {
	log := m.log.WithFields(logrus.Fields{"biName": req.Name, "biUUID": req.Uuid, "toAddress": req.ToAddress})
	log.Info("Backing Image Manager: prepare to send backing image")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Backing Image Manager: failed to start sending backing image")
		}
	}()

	if req.Name == "" || req.ToAddress == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	biFilePath := types.GetBackingImageFilePath(m.diskPath, req.Name, req.Uuid)
	if err := m.syncClient.Send(biFilePath, req.ToAddress); err != nil {
		return nil, err
	}

	log.Infof("Backing Image Manager: started sending backing image")
	return &empty.Empty{}, nil
}

func (m *Manager) Fetch(ctx context.Context, req *rpc.FetchRequest) (resp *rpc.BackingImageResponse, err error) {
	log := m.log.WithFields(logrus.Fields{"biName": req.Spec.Name, "biUUID": req.Spec.Uuid, "data_source_address": req.DataSourceAddress})
	log.Infof("Backing Image Manager: prepare to fetch backing image")

	defer func() {
		if err != nil {
			log.WithError(err).Error("Backing Image Manager: failed to start fetching backing image")
		}
	}()

	if req.Spec.Name == "" || req.Spec.Uuid == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	var srcFilePath string
	if req.DataSourceAddress != "" {
		log.Infof("Backing Image Manager: need to transfer the file from the data sourece server first")
		srcFilePath = types.GetDataSourceFilePath(m.diskPath, req.Spec.Name, req.Spec.Uuid)
		dsClient := &client.DataSourceClient{Remote: req.DataSourceAddress}
		dsInfo, err := dsClient.Get()
		if err != nil {
			return nil, err
		}
		if dsInfo.FilePath != srcFilePath || dsInfo.UUID != req.Spec.Uuid ||
			(dsInfo.State != string(types.StateReady) && dsInfo.State != string(types.StateReadyForTransfer)) {
			return nil, status.Errorf(codes.FailedPrecondition, "invalid data source file %v for fetch, uuid %v, state %v", dsInfo.FilePath, dsInfo.UUID, dsInfo.State)
		}
		if err := dsClient.Transfer(); err != nil {
			return nil, err
		}
	} else {
		log.Infof("Backing Image Manager: there is no need to transfer the file from the data sourece server, will try to directly reuse the file")
		srcFilePath = types.GetBackingImageFilePath(m.diskPath, req.Spec.Name, req.Spec.Uuid)
	}

	biFilePath := types.GetBackingImageFilePath(m.diskPath, req.Spec.Name, req.Spec.Uuid)
	if err := m.syncClient.Fetch(srcFilePath, biFilePath, req.Spec.Uuid, m.diskUUID, req.Spec.Checksum, req.Spec.Size); err != nil {
		return nil, err
	}

	log.Info("Backing Image Manager: fetched or reused backing image")
	return m.getAndUpdate(req.Spec.Name, req.Spec.Uuid)
}

func (m *Manager) PrepareDownload(ctx context.Context, req *rpc.PrepareDownloadRequest) (resp *rpc.PrepareDownloadResponse, err error) {
	log := m.log.WithFields(logrus.Fields{"biName": req.Name, "biUUID": req.Uuid})
	log.Infof("Backing Image Manager: start to make preparation for backing image download")

	defer func() {
		if err != nil {
			log.WithError(err).Error("Backing Image Manager: failed to make preparation for backing image download")
		}
	}()

	if req.Name == "" || req.Uuid == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument")
	}

	bi, err := m.getAndUpdate(req.Name, req.Uuid)
	if err != nil {
		return nil, err
	}

	if bi.Status.State != string(types.StateReady) {
		return nil, status.Errorf(codes.FailedPrecondition, "invalid backing image state %v for the download", bi.Status.State)
	}

	address, err := util.ConvertToStorageAddress(m.syncAddress)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "failed to detect the storage address of the sync server for the download: %v", err)
	}
	srcFilePath := types.GetBackingImageFilePath(m.diskPath, req.Name, req.Uuid)

	log.Infof("Backing Image Manager: prepared for backing image download, file path %v, address %v", srcFilePath, address)

	return &rpc.PrepareDownloadResponse{
		SrcFilePath: srcFilePath,
		Address:     address,
	}, nil
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
