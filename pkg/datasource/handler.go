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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

const (
	UploadBufferSize = 2 << 13
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

		fileName: fileName,
		filePath: filepath.Join(workDir, fileName),
		state:    types.StateStarting,

		downloader: downloader,
	}

	if err := s.init(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Service) init() error {
	switch s.sourceType {
	case types.DataSourceTypeDownload:
		return s.downloadFromURL(s.parameters)
	case types.DataSourceTypeUpload:
	default:
		return fmt.Errorf("unknown data source type: %v", s.sourceType)

	}
	return nil
}

func (s *Service) UpdateProgress(processedSize int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.state == types.StateStarting {
		s.state = types.StateInProgress
	}
	if s.state == types.StateReady {
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
	}()

	if err != nil {
		err = errors.Wrapf(err, "failed to finish file %v processing", s.filePath)
		return
	}

	if s.size > 0 && s.processedSize != s.size {
		err = fmt.Errorf("processed data size %v does not match the expected file size %v", s.processedSize, s.size)
		return
	}

	stat, statErr := os.Stat(s.filePath)
	if statErr != nil {
		err = errors.Wrapf(err, "failed to stat file %v after getting the file from source", s.filePath)
		return
	}

	checksum, cksumErr := util.GetFileChecksum(s.filePath)
	if cksumErr != nil {
		err = errors.Wrapf(cksumErr, "failed to calculate checksum for file %v getting the file from source", s.filePath)
		return
	}
	s.currentChecksum = checksum
	if s.expectedChecksum != "" && s.expectedChecksum != s.currentChecksum {
		err = fmt.Errorf("the expected checksum %v doesn't match the the file actual checksum %v", s.expectedChecksum, s.currentChecksum)
		return
	}

	s.lock.Lock()
	s.size = stat.Size()
	s.processedSize = stat.Size()
	s.progress = 100
	s.state = types.StateReady
	s.lock.Unlock()

	return
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
		_, err := s.downloader.DownloadFile(s.ctx, url, s.filePath, s)
		s.finishProcessing(err)
	}()

	return nil
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

	if err := os.Remove(s.filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(s.filePath, os.O_RDWR|os.O_CREATE, 0666)
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
