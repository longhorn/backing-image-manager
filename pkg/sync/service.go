package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/types"
)

const (
	AutoForgetCheckInterval = time.Hour
	AutoForgetWaitInterval  = 24 * time.Hour
)

type Service struct {
	lock *sync.RWMutex

	ctx context.Context
	log logrus.FieldLogger

	filePathMap map[string]*SyncingFile
	fileUUIDMap map[string]*SyncingFile

	// for unit test
	handler Handler
	sender  Sender
}

type Sender func(string, string) error

func InitService(ctx context.Context, listenAddr string, handler Handler) (*Service, error) {
	s := &Service{
		ctx: ctx,
		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component": "sync-server",
				"listen":    listenAddr,
			},
		),
		lock:        &sync.RWMutex{},
		filePathMap: map[string]*SyncingFile{},
		fileUUIDMap: map[string]*SyncingFile{},

		handler: handler,
		sender:  RequestBackingImageSending,
	}

	// go s.autoForget()

	s.log.Debugf("Sync Service: initialized")

	// TODO: Add websocket to notify the syncing file update

	return s, nil
}

func RequestBackingImageSending(filePath, receiverAddress string) error {
	return sparse.SyncFile(filePath, receiverAddress, types.FileSyncHTTPClientTimeout, false, false)
}

// func (s *Service) autoForget() {
// 	autoForgetWaitList := map[string]time.Time{}
// 	ticker := time.NewTicker(AutoForgetCheckInterval)
// 	defer ticker.Stop()
// 	for {
// 		now := time.Now()
// 		select {
// 		case <-s.ctx.Done():
// 			return
// 		case <-ticker.C:
// 			s.lock.Lock()
// 			for filePath, sf := range s.filePathMap {
// 				sfInfo := sf.Get()
// 				if sfInfo.State != string(types.StateReady) && sfInfo.State != string(types.StateFailed) {
// 					delete(autoForgetWaitList, filePath)
// 					continue
// 				}
// 				if _, exists := autoForgetWaitList[filePath]; !exists {
// 					autoForgetWaitList[filePath] = now
// 				}
// 				if now.After(autoForgetWaitList[filePath].Add(AutoForgetWaitInterval)) {
// 					s.log.Debugf("Sync Service: automatically forgot sync file %v, state %v", filePath, sfInfo.State)
// 					delete(s.filePathMap, filePath)
// 					delete(s.fileUUIDMap, sf.uuid)
// 				}
// 			}
// 			for filePath := range autoForgetWaitList {
// 				if _, exists := s.filePathMap[filePath]; !exists {
// 					delete(autoForgetWaitList, filePath)
// 				}
// 			}
// 			s.lock.Unlock()
// 		}
// 	}
// }

func (s *Service) List(writer http.ResponseWriter, request *http.Request) {
	// Deep copy
	filePathMap := make(map[string]*SyncingFile)
	s.lock.RLock()
	for filePath, sf := range s.filePathMap {
		filePathMap[filePath] = sf
	}
	s.lock.RUnlock()

	res := make(map[string]api.FileInfo, len(filePathMap))
	for filePath, sf := range filePathMap {
		res[filePath] = sf.Get()
	}

	outgoingJSON, err := json.Marshal(res)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(outgoingJSON)
}

func (s *Service) Get(writer http.ResponseWriter, request *http.Request) {
	encodedID := mux.Vars(request)["id"]
	filePath, err := url.QueryUnescape(encodedID)
	if err != nil {
		http.Error(writer, fmt.Sprintf("invalid id %v for decoding: %v", encodedID, err.Error()), http.StatusBadRequest)
		return
	}

	s.lock.RLock()
	sf := s.filePathMap[filePath]
	s.lock.RUnlock()

	if sf == nil {
		http.Error(writer, fmt.Sprintf("can not find sync file %v", filePath), http.StatusNotFound)
		return
	}

	outgoingJSON, err := json.Marshal(sf.Get())
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(outgoingJSON)
}

func (s *Service) Delete(writer http.ResponseWriter, request *http.Request) {
	if err := s.doCleanup(request, true); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
}

// Forget should be invoked by the caller when the file syncing is done
func (s *Service) Forget(writer http.ResponseWriter, request *http.Request) {
	if err := s.doCleanup(request, false); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
}

func (s *Service) doCleanup(request *http.Request, deleteFile bool) error {
	encodedID := mux.Vars(request)["id"]
	filePath, err := url.QueryUnescape(encodedID)
	if err != nil {
		return err
	}
	s.cleanup(filePath, deleteFile)
	return nil
}

func (s *Service) cleanup(filePath string, deleteFile bool) {
	if deleteFile {
		s.log.Infof("Sync Service: start cleaning up file %v, will delete the actual file", filePath)
	} else {
		s.log.Infof("Sync Service: start cleaning up file %v without deleting the actual file", filePath)
	}

	s.lock.Lock()
	sf := s.filePathMap[filePath]
	delete(s.filePathMap, filePath)
	if sf != nil {
		delete(s.fileUUIDMap, sf.uuid)
	}
	s.lock.Unlock()

	if sf != nil && deleteFile {
		sf.Delete()
	}
	return
}

func (s *Service) DownloadToDst(writer http.ResponseWriter, request *http.Request) {
	var err error
	defer func() {
		if err != nil {
			s.log.Errorf("Sync Service: failed to do download to dst, err: %v", err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
	}()

	var filePath string
	if filePath, err = url.QueryUnescape(mux.Vars(request)["id"]); err != nil {
		return
	}
	s.lock.RLock()
	sf := s.filePathMap[filePath]
	s.lock.RUnlock()

	if sf == nil {
		err = fmt.Errorf("nil sync file %v", filePath)
		return
	}

	src, sfErr := sf.GetFileReader()
	if sfErr != nil {
		err = sfErr
		return
	}
	defer src.Close()

	writer.Header().Set("Content-Disposition", "attachment; filename="+sf.uuid)
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Content-Length", strconv.FormatInt(sf.size, 10))
	if _, ioErr := io.Copy(writer, src); ioErr != nil {
		err = ioErr
		return
	}
	return
}

func (s *Service) checkAndInitSyncFile(filePath, uuid, diskUUID, expectedChecksum string, size int64) (*SyncingFile, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.filePathMap[filePath]; exists {
		return nil, fmt.Errorf("file %v already exists", filePath)
	}
	if _, exists := s.fileUUIDMap[uuid]; exists {
		return nil, fmt.Errorf("file %v with uuid %v already exists", filePath, uuid)
	}

	sf := NewSyncingFile(s.ctx, filePath, uuid, diskUUID, expectedChecksum, size, s.handler)
	s.filePathMap[filePath] = sf
	s.fileUUIDMap[uuid] = sf
	s.log.Debugf("Sync Service: initializing sync file %v", filePath)

	return sf, nil
}

func (s *Service) Fetch(writer http.ResponseWriter, request *http.Request) {
	err := s.doFetch(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Service) doFetch(request *http.Request) (err error) {
	if err != nil {
		s.log.Errorf("Sync Service: failed to do fetch, err: %v", err)
	}

	queryParams := request.URL.Query()
	srcFilePath := queryParams.Get("src-file-path")
	if srcFilePath == "" {
		return fmt.Errorf("no srcFilePath for existing file fetch")
	}
	dstFilePath := queryParams.Get("dst-file-path")
	if dstFilePath == "" {
		return fmt.Errorf("no dstFilePath for existing file fetch")
	}
	uuid := queryParams.Get("uuid")
	if uuid == "" {
		return fmt.Errorf("no uuid for existing file fetch")
	}
	diskUUID := queryParams.Get("disk-uuid")
	if diskUUID == "" {
		return fmt.Errorf("no diskUUID for existing file fetch")
	}
	expectedChecksum := queryParams.Get("expected-checksum")
	size, err := strconv.ParseInt(queryParams.Get("size"), 10, 64)
	if err != nil {
		return err
	}
	if size%types.DefaultSectorSize != 0 {
		return fmt.Errorf("the file size %d should be a multiple of %d bytes since Longhorn uses directIO by default", size, types.DefaultSectorSize)
	}

	sf, err := s.checkAndInitSyncFile(dstFilePath, uuid, diskUUID, expectedChecksum, size)
	if err != nil {
		return err
	}

	go func() {
		// Wait for the file reuse check & download preparation complete
		if err := sf.WaitForStateNonPending(); err != nil {
			s.log.Errorf("Sync Service: failed to wait for sync file %v becoming non-pending state during fetch: %v", dstFilePath, err)
			// SyncFile will mark itself as Failed if the processing is not started on time. There is no need to handle it here.
			return
		}

		if err := sf.Fetch(srcFilePath); err != nil {
			s.log.Errorf("Sync Service: failed to fetch sync file from %v to %v: %v", srcFilePath, dstFilePath, err)
			// SyncFile will mark itself as Failed if the processing is not started on time. There is no need to handle it here.
			return
		}
	}()

	return nil
}

func (s *Service) DownloadFromURL(writer http.ResponseWriter, request *http.Request) {
	err := s.doDownloadFromURL(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Service) doDownloadFromURL(request *http.Request) (err error) {
	defer func() {
		if err != nil {
			s.log.Errorf("Sync Service: failed to do download from URL, err: %v", err)
		}
	}()

	queryParams := request.URL.Query()
	filePath := queryParams.Get("file-path")
	if filePath == "" {
		return fmt.Errorf("no filePath for file downloading")
	}
	uuid := queryParams.Get("uuid")
	if uuid == "" {
		return fmt.Errorf("no uuid for downloading file")
	}
	url := queryParams.Get("url")
	if url == "" {
		return fmt.Errorf("no URL for file downloading")
	}
	diskUUID := queryParams.Get("disk-uuid")
	expectedChecksum := queryParams.Get("expected-checksum")

	sf, err := s.checkAndInitSyncFile(filePath, uuid, diskUUID, expectedChecksum, 0)
	if err != nil {
		return err
	}

	go func() {
		// Wait for the file reuse check & download preparation complete
		if err := sf.WaitForStateNonPending(); err != nil {
			s.log.Errorf("Sync Service: failed to wait for sync file %v becoming non-pending state before starting the actual download: %v", filePath, err)
			// SyncFile will mark itself as Failed if the processing is not started on time. There is no need to handle it here.
			return
		}

		if _, err := sf.DownloadFromURL(url); err != nil {
			s.log.Errorf("Sync Service: failed to download sync file %v: %v", filePath, err)
			return
		}
	}()

	return nil
}

func (s *Service) UploadFromRequest(writer http.ResponseWriter, request *http.Request) {
	err := s.doUploadFromRequest(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Service) doUploadFromRequest(request *http.Request) (err error) {
	defer func() {
		if err != nil {
			s.log.Errorf("Sync Service: failed to do upload, err: %v", err)
		}
	}()

	// Get parameters from the request
	queryParams := request.URL.Query()
	filePath := queryParams.Get("file-path")
	if filePath == "" {
		return fmt.Errorf("no file-path for uploading file")
	}
	uuid := queryParams.Get("uuid")
	if uuid == "" {
		return fmt.Errorf("no uuid for uploading file")
	}
	diskUUID := queryParams.Get("disk-uuid")
	expectedChecksum := queryParams.Get("expected-checksum")
	size, err := strconv.ParseInt(queryParams.Get("size"), 10, 64)
	if err != nil {
		return err
	}
	if size%types.DefaultSectorSize != 0 {
		return fmt.Errorf("the uploaded file size %d should be a multiple of %d bytes since Longhorn uses directIO by default", size, types.DefaultSectorSize)
	}

	sf, err := s.checkAndInitSyncFile(filePath, uuid, diskUUID, expectedChecksum, size)
	if err != nil {
		return err
	}

	s.log.Info("Sync Service: start uploading file")

	// Prepare the src/reader
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
			s.log.Warnf("Sync Service: unexpected form %v in upload request, will ignore it", p.FormName())
			continue
		}
		break
	}
	if p == nil || p.FormName() != "chunk" {
		return fmt.Errorf("cannot get the uploaded data since the upload request doesn't contain form 'chunk'")
	}
	defer p.Close()

	if err := sf.WaitForStateNonPending(); err != nil {
		s.log.Errorf("Sync Service: failed to wait for sync file %v becoming non-pending state before starting the actual upload: %v", filePath, err)
		// SyncFile will mark itself as Failed if the processing is not started on time. There is no need to handle it here.
		return err
	}

	if _, err := sf.IdleTimeoutCopyToFile(p); err != nil {
		return err
	}

	return nil
}

func (s *Service) ReceiveFromPeer(writer http.ResponseWriter, request *http.Request) {
	err := s.doReceiveFromPeer(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Service) doReceiveFromPeer(request *http.Request) (err error) {
	if err != nil {
		s.log.Errorf("Sync Service: failed to do receive from peer, err: %v", err)
	}

	queryParams := request.URL.Query()
	filePath := queryParams.Get("file-path")
	if filePath == "" {
		return fmt.Errorf("no filePath for file requesting")
	}
	uuid := queryParams.Get("uuid")
	if uuid == "" {
		return fmt.Errorf("no uuid for file requesting")
	}
	diskUUID := queryParams.Get("disk-uuid")
	expectedChecksum := queryParams.Get("expected-checksum")
	fileType := queryParams.Get("file-type")
	if fileType == types.SyncingFileTypeEmpty {
		fileType = types.SyncingFileTypeRaw
	}
	if fileType != types.SyncingFileTypeRaw && fileType != types.SyncingFileTypeQcow2 {
		return fmt.Errorf("")
	}
	size, err := strconv.ParseInt(queryParams.Get("size"), 10, 64)
	if err != nil {
		return err
	}
	if size%types.DefaultSectorSize != 0 {
		return fmt.Errorf("the uploaded file size %d should be a multiple of %d bytes since Longhorn uses directIO by default", size, types.DefaultSectorSize)
	}
	port, err := strconv.ParseInt(queryParams.Get("port"), 10, 64)
	if err != nil {
		return err
	}

	sf, err := s.checkAndInitSyncFile(filePath, uuid, diskUUID, expectedChecksum, size)
	if err != nil {
		return err
	}

	go func() {
		// Wait for the file reuse check & receive preparation complete
		if err := sf.WaitForStateNonPending(); err != nil {
			s.log.Errorf("Sync Service: failed to wait for sync file %v becoming non-pending state before the receiving: %v", filePath, err)
			// SyncFile will mark itself as Failed if the processing is not started on time. There is no need to handle it here.
			return
		}

		if err := sf.Receive(int(port), fileType); err != nil {
			s.log.Errorf("Sync Service: failed to receive sync file %v: %v", filePath, err)
			return
		}
	}()

	return nil
}

func (s *Service) SendToPeer(writer http.ResponseWriter, request *http.Request) {
	err := s.doSendToPeer(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Service) doSendToPeer(request *http.Request) (err error) {
	if err != nil {
		s.log.Errorf("Sync Service: failed to do send to peer, err: %v", err)
	}

	filePath, err := url.QueryUnescape(mux.Vars(request)["id"])
	if err != nil {
		return err
	}
	if filePath == "" {
		return fmt.Errorf("no filePath for file sending")
	}
	toAddress := request.URL.Query().Get("to-address")
	if toAddress == "" {
		return fmt.Errorf("no toAddress for file sending")
	}

	s.lock.RLock()
	sf := s.filePathMap[filePath]
	s.lock.RUnlock()

	if sf == nil {
		return fmt.Errorf("can not find sync file %v for sending", filePath)
	}

	return sf.Send(toAddress, s.sender)
}
