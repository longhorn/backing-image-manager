package datasource

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	repclient "github.com/longhorn/longhorn-engine/pkg/replica/client"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

const (
	RetryInterval = time.Second
	RetryCount    = 60

	TimeoutBeginErrorMessage = "timeout waiting for the datasource file processing begin"
)

type Service struct {
	ctx    context.Context
	cancel context.CancelFunc
	lock   *sync.RWMutex
	log    logrus.FieldLogger

	timeoutBegin bool
	transferred  bool
	dsInfo       *api.DataSourceInfo

	filePath         string
	name             string
	uuid             string
	diskUUID         string
	sourceType       types.DataSourceType
	parameters       map[string]string
	credential       map[string]string
	expectedChecksum string

	syncListenAddr string
	syncClient     client.SyncClient
}

func LaunchService(ctx context.Context, cancel context.CancelFunc,
	syncListenAddr, checksum, sourceType, name, uuid, diskPathInContainer string,
	parameters map[string]string, credential map[string]string) (*Service, error) {

	if name == "" || uuid == "" {
		return nil, fmt.Errorf("the backing image name or uuid is not specified")
	}
	diskUUID, err := util.GetDiskConfig(diskPathInContainer)
	if err != nil {
		return nil, err
	}
	dsFilePath := types.GetDataSourceFilePath(diskPathInContainer, name, uuid)
	workDir := filepath.Dir(dsFilePath)
	if err := os.MkdirAll(workDir, 0666); err != nil && !os.IsExist(err) {
		return nil, err
	}

	if parameters == nil {
		parameters = make(map[string]string)
	}

	s := &Service{
		ctx:    ctx,
		cancel: cancel,
		lock:   &sync.RWMutex{},

		filePath:         dsFilePath,
		name:             name,
		uuid:             uuid,
		diskUUID:         diskUUID,
		sourceType:       types.DataSourceType(sourceType),
		parameters:       parameters,
		credential:       credential,
		expectedChecksum: checksum,

		syncListenAddr: syncListenAddr,
		syncClient: client.SyncClient{
			Remote: syncListenAddr,
		},
	}
	s.dsInfo = &api.DataSourceInfo{
		SourceType: string(s.sourceType),
		Parameters: s.parameters,

		Name: s.name,

		FileInfo: api.FileInfo{
			DiskUUID:         s.diskUUID,
			ExpectedChecksum: s.expectedChecksum,

			FilePath: s.filePath,
			UUID:     uuid,
			State:    "",
		},
	}
	s.log = logrus.StandardLogger().WithFields(
		logrus.Fields{
			"component":        "data-source-service",
			"filePath":         s.filePath,
			"name":             s.name,
			"uuid":             s.uuid,
			"sourceType":       s.sourceType,
			"diskUUID":         s.diskUUID,
			"parameters":       s.parameters,
			"expectedChecksum": s.expectedChecksum,
		},
	)

	s.log.Debugf("DataSource Service: initializing")
	if err := s.init(); err != nil {
		s.log.Errorf("DataSource Service: failed at initialization: %v", err)
		return nil, err
	}
	s.log.Debugf("DataSource Service: initialized")

	return s, nil
}

func (s *Service) init() (err error) {
	defer func() {
		go s.waitForBeginning()
		if err != nil {
			s.log.Errorf("DataSource Service: failed to init data source file: %v", err)
		}
	}()

	switch s.sourceType {
	case types.DataSourceTypeDownload:
		return s.downloadFromURL(s.parameters)
	case types.DataSourceTypeUpload:
		return s.prepareForUpload()
	case types.DataSourceTypeExportFromVolume:
		return s.exportFromVolume(s.parameters)
	case types.DataSourceTypeRestore:
		return s.restoreFromBackupURL()
	case types.DataSourceTypeClone:
		return s.cloneFromBackingImage()
	default:
		return fmt.Errorf("unknown data source type: %v", s.sourceType)
	}
}

func (s *Service) waitForBeginning() {
	count := 0
	ticker := time.NewTicker(RetryInterval)
	defer ticker.Stop()
	for {
		<-ticker.C
		if count >= RetryCount {
			s.lock.Lock()
			s.timeoutBegin = true
			s.dsInfo.State = string(types.StateFailed)
			s.dsInfo.Message = TimeoutBeginErrorMessage
			s.lock.Unlock()
			if err := s.syncClient.Delete(s.filePath); err != nil {
				s.log.WithError(err).Error("DataSource Service: failed to do cleanup after timeout waiting for the datasource processing begin")
			}
			return
		}

		count++

		dsInfo, err := s.syncDataSourceFileInfo()
		if err != nil {
			s.log.Debugf("DataSource Service: failed to get the datasource file info, the processing may be not begin yet: %v", err)
			continue
		}

		notBeginYet := dsInfo.State == "" || dsInfo.State == string(types.StatePending) || dsInfo.State == string(types.StateStarting)
		if !notBeginYet {
			return
		}
		s.log.Debugf("DataSource Service: datasource file is state %v, the processing is not begin yet", dsInfo.State)
	}
}

func (s *Service) cloneFromBackingImage() (err error) {
	sourceBackingImage := s.parameters[types.DataSourceTypeCloneParameterBackingImage]
	if sourceBackingImage == "" {
		return fmt.Errorf("%v is not specified", types.DataSourceTypeCloneParameterBackingImage)
	}

	sourceBackingImageUUID := s.parameters[types.DataSourceTypeCloneParameterBackingImageUUID]
	if sourceBackingImageUUID == "" {
		return fmt.Errorf("%v is not specified", types.DataSourceTypeCloneParameterBackingImageUUID)
	}

	dataEngine := s.parameters[types.DataSourceTypeParameterDataEngine]
	if dataEngine == "" {
		dataEngine = types.DataEnginev1
	}

	encryption := s.parameters[types.DataSourceTypeCloneParameterEncryption]
	if types.EncryptionType(encryption) != types.EncryptionTypeEncrypt &&
		types.EncryptionType(encryption) != types.EncryptionTypeDecrypt &&
		types.EncryptionType(encryption) != types.EncryptionTypeIgnore {
		return fmt.Errorf("%v operation %v is not specified", types.DataSourceTypeCloneParameterEncryption, encryption)
	}

	if types.EncryptionType(encryption) == types.EncryptionTypeEncrypt ||
		types.EncryptionType(encryption) == types.EncryptionTypeDecrypt {
		if len(s.credential) == 0 {
			return fmt.Errorf("secret is not provided for %v or %v", types.EncryptionTypeEncrypt, types.EncryptionTypeDecrypt)
		}
	}

	return s.syncClient.CloneFromBackingImage(sourceBackingImage, sourceBackingImageUUID, encryption, s.filePath, s.uuid, s.diskUUID, s.expectedChecksum, s.credential, dataEngine)
}

func (s *Service) restoreFromBackupURL() (err error) {
	backupURL := s.parameters[types.DataSourceTypeRestoreParameterBackupURL]
	if backupURL == "" {
		return fmt.Errorf("no %v for restore", types.DataSourceTypeRestoreParameterBackupURL)
	}
	concurrentLimit := s.parameters[types.DataSourceTypeRestoreParameterConcurrentLimit]
	if concurrentLimit == "" {
		return fmt.Errorf("no %v for restore", types.DataSourceTypeRestoreParameterConcurrentLimit)
	}

	dataEngine := s.parameters[types.DataSourceTypeParameterDataEngine]
	if dataEngine == "" {
		dataEngine = types.DataEnginev1
	}

	return s.syncClient.RestoreFromBackupURL(backupURL, concurrentLimit, s.filePath, s.uuid, s.diskUUID, s.expectedChecksum, s.credential, dataEngine)
}

func (s *Service) downloadFromURL(parameters map[string]string) (err error) {
	url := parameters[types.DataSourceTypeDownloadParameterURL]
	if url == "" {
		return fmt.Errorf("no URL for file downloading")
	}
	dataEngine := parameters[types.DataSourceTypeParameterDataEngine]
	if dataEngine == "" {
		dataEngine = types.DataEnginev1
	}

	return s.syncClient.DownloadFromURL(url, s.filePath, s.uuid, s.diskUUID, s.expectedChecksum, dataEngine)
}

func (s *Service) prepareForUpload() (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.dsInfo.State != "" {
		return fmt.Errorf("datasource file is already state %v before init complete", s.dsInfo.State)
	}
	s.dsInfo.State = string(types.StatePending)

	return nil
}

func (s *Service) exportFromVolume(parameters map[string]string) error {
	snapshotName := parameters[types.DataSourceTypeExportFromVolumeParameterSnapshotName]
	if snapshotName == "" {
		return fmt.Errorf("snapshot name is not specified during volume exporting")
	}
	senderAddress := parameters[types.DataSourceTypeExportFromVolumeParameterSenderAddress]
	if senderAddress == "" {
		return fmt.Errorf("available replica address of the source volume is not specified during volume exporting")
	}
	fileType := parameters[types.DataSourceTypeFileType]

	dataEngine := parameters[types.DataSourceTypeParameterDataEngine]
	if dataEngine == "" {
		dataEngine = types.DataEnginev1
	}

	var size int64
	var err error
	if size, err = strconv.ParseInt(parameters[types.DataSourceTypeExportFromVolumeParameterVolumeSize], 10, 64); err != nil {
		s.log.Warnf("DataSource Service: Failed to parse string %v to an invalid number as size, will ignore this input parameter: %v",
			parameters[types.DataSourceTypeExportFromVolumeParameterVolumeSize], err)
	}

	var timeout int
	if timeout, err = strconv.Atoi(parameters[types.DataSourceTypeExportFromVolumeParameterFileSyncHTTPClientTimeout]); err != nil {
		s.log.Warnf("DataSource Service: Failed to parse string %v to a valid timeout integer, will ignore this input parameter: %v",
			parameters[types.DataSourceTypeExportFromVolumeParameterFileSyncHTTPClientTimeout], err)
		timeout = 60
	}

	// TODO: Use the storage IP of the sync service after launching the separate sync server pod.
	storageIP, err := util.GetIPForPod()
	if err != nil {
		return fmt.Errorf("failed to get an available ip during volume export")
	}
	s.log.Infof("DataSource Service: export volume via %v", storageIP)

	if err := s.syncClient.Receive(s.filePath, s.uuid, s.diskUUID, s.expectedChecksum, fileType, types.DefaultVolumeExportReceiverPort, size, dataEngine); err != nil {
		return err
	}

	go func() {
		var senderErr error
		defer func() {
			if senderErr != nil {
				s.log.Errorf("DataSource Service: failed to ask the sender volume to send data: %v", senderErr)
			}
		}()

		replicaClient, err := repclient.NewReplicaClient(senderAddress, "", "")
		if err != nil {
			senderErr = errors.Wrapf(err, "failed to get replica client %v", senderAddress)
			return
		}
		if err := replicaClient.ExportVolume(snapshotName, storageIP, types.DefaultVolumeExportReceiverPort, true, timeout); err != nil {
			senderErr = errors.Wrapf(err, "failed to export volume snapshot %v", snapshotName)
			return
		}
	}()

	return nil
}

func (s *Service) Upload(writer http.ResponseWriter, request *http.Request) {
	if s.sourceType != types.DataSourceTypeUpload {
		http.Error(writer, fmt.Sprintf("cannot do upload since data source type is %v rather than upload", s.sourceType), http.StatusBadRequest)
		return
	}

	request.Host = s.syncListenAddr
	request.URL.Host = s.syncListenAddr
	request.URL.Scheme = "http"
	request.URL.Path = "/v1/files"
	q := request.URL.Query()
	q.Add("file-path", s.filePath)
	q.Add("uuid", s.uuid)
	q.Add("expected-checksum", s.expectedChecksum)

	dataEngine := s.parameters[types.DataSourceTypeParameterDataEngine]
	if dataEngine == "" {
		dataEngine = types.DataEnginev1
	}
	q.Add("data-engine", dataEngine)

	request.URL.RawQuery = q.Encode()
	s.log.Debugf("DataSource Service: forwarding upload request to sync server %v", request.URL.String())

	proxy := &httputil.ReverseProxy{Director: func(r *http.Request) {}}
	proxy.ServeHTTP(writer, request)
}

func (s *Service) Get(writer http.ResponseWriter, request *http.Request) {
	dsInfo, err := s.syncDataSourceFileInfo()
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	outgoingJSON, err := json.Marshal(dsInfo)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if _, err := writer.Write(outgoingJSON); err != nil {
		logrus.WithError(err).Warn("Failed to write response")
	}
}

func (s *Service) Transfer(writer http.ResponseWriter, request *http.Request) {
	if s.isTransferred() {
		return
	}

	dsInfo, err := s.syncDataSourceFileInfo()
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	if dsInfo.State != string(types.StateReadyForTransfer) {
		http.Error(writer, fmt.Sprintf("datasource file current state is %v rather than %v, cannot do transfer", dsInfo.State, types.StateReadyForTransfer), http.StatusInternalServerError)
		return
	}

	if err := s.syncClient.Forget(s.filePath); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	s.lock.Lock()
	s.transferred = true
	s.dsInfo.State = string(types.StateReady)
	s.lock.Unlock()
}

func (s *Service) isTransferred() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.transferred
}

func (s *Service) isTimeoutBegin() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.timeoutBegin
}

func (s *Service) syncDataSourceFileInfo() (*api.DataSourceInfo, error) {
	if s.isTransferred() || s.isTimeoutBegin() {
		s.lock.RLock()
		defer s.lock.RUnlock()
		return s.dsInfo.DeepCopy(), nil
	}

	fInfo, err := s.syncClient.Get(s.filePath)
	if err != nil {
		// To inform the caller that the data source server is ready for uploading,
		// this server should return a valid file state ("pending").
		// And the sync server should not contain the upload file info before actual receiving starts.
		if s.sourceType == types.DataSourceTypeUpload && util.IsHTTPClientErrorNotFound(err) {
			s.lock.RLock()
			defer s.lock.RUnlock()
			return s.dsInfo.DeepCopy(), nil
		}
		return nil, err
	}
	dsInfo := &api.DataSourceInfo{
		SourceType: string(s.sourceType),
		Parameters: s.parameters,

		Name: s.name,

		FileInfo: *fInfo,
	}
	// dsInfo.State can be marked as ready when the transfer is done.
	if dsInfo.State == string(types.StateReady) {
		dsInfo.State = string(types.StateReadyForTransfer)
	}

	s.lock.Lock()
	s.dsInfo = dsInfo.DeepCopy()
	s.lock.Unlock()

	return dsInfo, nil
}
