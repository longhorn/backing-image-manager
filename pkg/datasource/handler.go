package datasource

import (
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	sparserest "github.com/longhorn/sparse-tools/sparse/rest"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

const (
	RetryInterval = 1 * time.Second
	RetryCount    = 60
)

type Service struct {
	ctx  context.Context
	lock *sync.RWMutex
	log  logrus.FieldLogger

	diskUUID         string
	sourceType       types.DataSourceType
	parameters       map[string]string
	expectedChecksum string

	fileName        string
	filePath        string
	tmpFilePath     string
	state           types.State
	size            int64
	progress        int
	processedSize   int64
	currentChecksum string
	message         string

	// for unit test
	downloader HTTPDownloader
}

func LaunchService(ctx context.Context,
	fileName, checksum, sourceType string, parameters map[string]string,
	diskPathInContainer string, downloader HTTPDownloader) (*Service, error) {
	if fileName == "" {
		return nil, fmt.Errorf("the file name is not specified")
	}
	diskUUID, err := util.GetDiskConfig(diskPathInContainer)
	if err != nil {
		return nil, err
	}
	workDir := filepath.Join(diskPathInContainer, types.DataSourceDirectoryName)
	if err := os.Mkdir(workDir, 0666); err != nil && !os.IsExist(err) {
		return nil, err
	}

	s := &Service{
		ctx:  ctx,
		lock: &sync.RWMutex{},
		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component": "data-source",
			},
		),

		diskUUID:         diskUUID,
		sourceType:       types.DataSourceType(sourceType),
		parameters:       parameters,
		expectedChecksum: checksum,

		fileName:    fileName,
		filePath:    filepath.Join(workDir, fileName),
		tmpFilePath: filepath.Join(workDir, fileName+types.TmpFileSuffix),
		state:       types.StateStarting,

		downloader: downloader,
	}

	s.log.Debugf("Initializing data source service")
	if err := s.init(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Service) init() error {
	if err := s.checkAndReuseBackingImageFile(); err == nil {
		return nil
	}
	if err := os.RemoveAll(s.filePath); err != nil {
		return err
	}
	if err := os.RemoveAll(s.tmpFilePath); err != nil {
		return err
	}
	switch s.sourceType {
	case types.DataSourceTypeDownload:
		return s.downloadFromURL(s.parameters)
	case types.DataSourceTypeUpload:
	case types.DataSourceTypeExportFromVolume:
		return s.exportFromVolume(s.parameters)
	default:
		return fmt.Errorf("unknown data source type: %v", s.sourceType)

	}
	go s.waitForProcessingStart()

	return nil
}

func (s *Service) checkAndReuseBackingImageFile() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	info, err := os.Stat(s.filePath)
	if err != nil {
		return err
	}
	checksum, err := util.GetFileChecksum(s.filePath)
	if err != nil {
		return errors.Wrapf(err, "failed to calculate checksum for the existing file after processing")
	}
	s.currentChecksum = checksum
	if s.expectedChecksum != "" && s.expectedChecksum != s.currentChecksum {
		return fmt.Errorf("backing image expected checksum %v doesn't match the existing file checksum %v", s.expectedChecksum, s.currentChecksum)
	}

	s.size = info.Size()
	s.processedSize = s.size
	s.progress = 100
	s.state = types.StateReadyForTransfer
	s.log = s.log.WithFields(logrus.Fields{
		"size":            s.size,
		"currentChecksum": s.currentChecksum,
	})

	s.log.Infof("Directly reuse/introduce the existing file in path %v", s.filePath)

	return nil
}

func (s *Service) UpdateProgress(processedSize int64) {
	s.updateProgress(processedSize)
}

func (s *Service) UpdateSyncFileProgress(size int64) {
	s.updateProgress(size)
}

func (s *Service) updateProgress(processedSize int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.state == types.StateStarting {
		s.state = types.StateInProgress
	}
	if s.state == types.StateReady || s.state == types.StateReadyForTransfer {
		return
	}

	s.processedSize = s.processedSize + processedSize
	if s.size > 0 {
		s.progress = int((float32(s.processedSize) / float32(s.size)) * 100)
	}
}

func (s *Service) finishProcessing(err error) {
	defer func() {
		if err != nil {
			s.lock.Lock()
			s.state = types.StateFailed
			s.message = err.Error()
			s.lock.Unlock()
		}
		os.RemoveAll(s.tmpFilePath)
	}()

	if err != nil {
		err = errors.Wrapf(err, "failed to finish file %v and %v processing", s.tmpFilePath, s.filePath)
		return
	}

	if s.size > 0 && s.processedSize != s.size {
		err = fmt.Errorf("processed data size %v does not match the expected file size %v", s.processedSize, s.size)
		return
	}

	stat, statErr := os.Stat(s.tmpFilePath)
	if statErr != nil {
		err = errors.Wrapf(err, "failed to stat file %v after getting the file from source", s.tmpFilePath)
		return
	}

	checksum, cksumErr := util.GetFileChecksum(s.tmpFilePath)
	if cksumErr != nil {
		err = errors.Wrapf(cksumErr, "failed to calculate checksum for file %v getting the file from source", s.tmpFilePath)
		return
	}
	s.currentChecksum = checksum
	if s.expectedChecksum != "" && s.expectedChecksum != s.currentChecksum {
		err = fmt.Errorf("the expected checksum %v doesn't match the the file actual checksum %v", s.expectedChecksum, s.currentChecksum)
		return
	}
	if err := os.Rename(s.tmpFilePath, s.filePath); err != nil {
		err = fmt.Errorf("failed to rename the tmp file %v to %v at the end of processing", s.tmpFilePath, s.filePath)
		return
	}

	s.lock.Lock()
	s.size = stat.Size()
	s.processedSize = stat.Size()
	s.progress = 100
	s.state = types.StateReadyForTransfer
	s.lock.Unlock()
}

func (s *Service) downloadFromURL(parameters map[string]string) error {
	url := parameters[types.DataSourceTypeDownloadParameterURL]
	if url == "" {
		return fmt.Errorf("no URL for file downloading")
	}

	size, err := s.downloader.GetDownloadSize(url)
	if err != nil {
		return err
	}
	if size > 0 {
		s.lock.Lock()
		s.size = size
		s.lock.Unlock()
	}

	go func() {
		_, err := s.downloader.DownloadFile(s.ctx, url, s.tmpFilePath, s)
		s.finishProcessing(err)
	}()

	return nil
}

func (s *Service) exportFromVolume(parameters map[string]string) error {
	snapshotName := parameters[types.DataSourceTypeExportFromVolumeParameterSnapshotName]
	if snapshotName == "" {
		return fmt.Errorf("snapshot name is not specified during volume exporting")
	}
	senderAddress := parameters[types.DataSourceTypeExportFromVolumeParameterSenderAddress]
	if senderAddress == "" {
		return fmt.Errorf("avaialble replica address of the source volume is not specified during volume exporting")
	}

	qcow2ConversionRequired := false
	if parameters[types.DataSourceTypeExportFromVolumeParameterExportType] == types.DataSourceTypeExportFromVolumeParameterExportTypeQCOW2 {
		qcow2ConversionRequired = true
	}

	var size int64
	var err error
	volumeSizeStr := parameters[types.DataSourceTypeExportFromVolumeParameterVolumeSize]
	if volumeSizeStr != "" {
		if size, err = strconv.ParseInt(volumeSizeStr, 10, 64); err != nil {
			s.log.Warnf("failed to parse string %v to an invalid number as size, will ignore this input parameter: %v", volumeSizeStr, err)
		}
	}

	podIP := os.Getenv(types.EnvPodIP)
	if podIP == "" {
		return fmt.Errorf("can't get pod ip from environment variable")
	}

	s.lock.Lock()
	s.size = size
	s.lock.Unlock()

	errChan := make(chan error, 1)
	ctx, cancel := context.WithCancel(s.ctx)
	go func() {
		var syncErr error
		defer func() {
			if syncErr != nil {
				s.log.Errorf("failed to receive backing image file from snapshot %v: %v", snapshotName, syncErr)
			}
			if syncErr == nil {
				syncErr = <-errChan
			}
			cancel()
			s.finishProcessing(syncErr)
		}()

		if err := sparserest.Server(ctx, strconv.Itoa(types.DefaultVolumeExportReceiverPort), s.tmpFilePath, s); err != nil && err != http.ErrServerClosed {
			syncErr = err
			return
		}

		// For volume export, the size of holes won't be calculated into s.processedSize.
		// To avoid failing s.finishProcessing, we need to update s.processedSize in advance.
		s.lock.Lock()
		s.processedSize = s.size
		s.lock.Unlock()

		// The file size will change after conversion.
		if qcow2ConversionRequired {
			if syncErr = util.ConvertFromRawToQcow2(s.tmpFilePath); syncErr != nil {
				return
			}
		}
	}()

	go func() {
		var senderErr error
		defer func() {
			if senderErr != nil {
				s.log.Errorf("failed to ask replica %v to send backing image file data: %v", senderAddress, senderErr)
				cancel()
			}
			errChan <- senderErr
			close(errChan)
		}()
		replicaClient, err := client.NewReplicaClient(senderAddress)
		if err != nil {
			senderErr = errors.Wrapf(err, "failed to get replica client %v", senderAddress)
			return
		}
		if err := replicaClient.ExportVolume(snapshotName, podIP, types.DefaultVolumeExportReceiverPort, true); err != nil {
			senderErr = errors.Wrapf(err, "failed to export volume snapshot %v", snapshotName)
			return
		}
	}()

	return nil
}

func (s *Service) waitForProcessingStart() {
	count := 0
	ticker := time.NewTicker(RetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			count++
			s.lock.Lock()
			if s.state == types.StateInProgress || s.state == types.StateReadyForTransfer || s.state == types.StateFailed {
				s.lock.Unlock()
				return
			}
			if count >= RetryCount {
				s.state = types.StateFailed
				s.message = fmt.Sprintf("failed to wait for processing start in %v seconds", RetryCount)
				s.lock.Unlock()
				return
			}
			s.lock.Unlock()
		}
	}
}

func (s *Service) Upload(writer http.ResponseWriter, request *http.Request) {
	err := s.doUpload(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Service) doUpload(request *http.Request) (err error) {
	if s.sourceType != types.DataSourceTypeUpload {
		return fmt.Errorf("cannot do upload since data source type is %v rather than upload", s.sourceType)
	}

	queryParams := request.URL.Query()
	size, err := strconv.ParseInt(queryParams.Get("size"), 10, 64)
	if err != nil {
		return err
	}
	if size%types.DefaultSectorSize != 0 {
		return fmt.Errorf("the uploaded file size %d should be a multiple of %d bytes since Longhorn uses directIO by default", size, types.DefaultSectorSize)
	}
	s.lock.Lock()
	s.size = size
	s.lock.Unlock()

	s.log.Info("Start uploading file")

	reader, err := request.MultipartReader()
	if err != nil {
		return err
	}
	var p *multipart.Part
	for {
		if p, err = reader.NextPart(); err != nil {
			return err
		}
		if p.FormName() != "chunk" {
			s.log.Warnf("Unexpected form %v in upload request, will ignore it", p.FormName())
			continue
		}
		break
	}
	if p == nil || p.FormName() != "chunk" {
		return fmt.Errorf("cannot get the uploaded data since the upload request doesn't contain form 'chunk'")
	}

	if err := os.Remove(s.tmpFilePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(s.tmpFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = f.Truncate(size); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	go func() {
		<-s.ctx.Done()
		p.Close()
	}()
	defer s.finishProcessing(err)
	if _, err := IdleTimeoutCopy(ctx, cancel, p, f, s); err != nil {
		return errors.Wrapf(err, "failed to upload file")
	}

	return nil
}

func (s *Service) Get(writer http.ResponseWriter, request *http.Request) {
	s.lock.RLock()
	s.lock.RUnlock()
	fi := api.DataSourceInfo{
		DiskUUID:         s.diskUUID,
		SourceType:       string(s.sourceType),
		Parameters:       s.parameters,
		ExpectedChecksum: s.expectedChecksum,

		FileName:        s.fileName,
		State:           string(s.state),
		Size:            s.size,
		Progress:        s.progress,
		ProcessedSize:   s.processedSize,
		CurrentChecksum: s.currentChecksum,
		Message:         s.message,
	}

	outgoingJSON, err := json.Marshal(fi)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(outgoingJSON)
}
