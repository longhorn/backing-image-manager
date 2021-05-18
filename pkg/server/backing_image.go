package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/pkg/rpc"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

type state string

const (
	StatePending    = state(types.BackingImageStatePending)
	StateStarting   = state(types.BackingImageStateStarting)
	StateInProgress = state(types.BackingImageStateInProgress)
	StateReady      = state(types.BackingImageStateReady)
	StateFailed     = state(types.BackingImageStateFailed)

	RetryInterval = 1 * time.Second
	RetryCount    = 30
)

type BackingImage struct {
	Name          string
	URL           string
	UUID          string
	HostDirectory string
	WorkDirectory string
	state         state
	errorMsg      string

	size          int64
	processedSize int64
	progress      int

	sendingReference     int
	senderManagerAddress string

	uploadPort int32

	// Need to acquire lock when access to BackingImage fields as well as its meta file.
	lock *sync.RWMutex

	log      logrus.FieldLogger
	updateCh chan interface{}

	handler *BackingImageHandler
}

func NewBackingImage(name, url, uuid, diskPathOnHost, diskPathInContainer string, handler *BackingImageHandler, updateCh chan interface{}) *BackingImage {
	hostDir := filepath.Join(diskPathOnHost, types.BackingImageManagerDirectoryName, GetBackingImageDirectoryName(name, uuid))
	workDir := filepath.Join(diskPathInContainer, types.BackingImageManagerDirectoryName, GetBackingImageDirectoryName(name, uuid))
	return &BackingImage{
		Name:          name,
		UUID:          uuid,
		URL:           url,
		HostDirectory: hostDir,
		WorkDirectory: workDir,
		state:         types.BackingImageStatePending,
		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component": "backing-image",
				"name":      name,
				"url":       url,
				"uuid":      uuid,
				"hostDir":   hostDir,
				"workDir":   workDir,
			},
		),
		lock:     &sync.RWMutex{},
		handler:  handler,
		updateCh: updateCh,
	}
}

func GetBackingImageDirectoryName(biName, biUUID string) string {
	return fmt.Sprintf("%s-%s", biName, biUUID)
}

func (bi *BackingImage) Pull() (resp *rpc.BackingImageResponse, err error) {
	bi.lock.Lock()
	log := bi.log
	log.Info("Backing Image: start to pull backing image")

	if bi.state != types.BackingImageStatePending {
		state := bi.state
		bi.lock.Unlock()
		return nil, fmt.Errorf("invalid state %v for pulling", state)
	}

	defer func() {
		if err != nil {
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			log.WithError(err).Error("Backing Image: failed to pull backing image")
			bi.handler.Cancel()
		}
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	// This means state was pending but somehow the handler had been initialized.
	if err := bi.handler.InitProcessing(); err != nil {
		return nil, errors.Wrapf(err, "failed to ask for the handler init before pulling")
	}
	bi.state = types.BackingImageStateStarting

	if err = bi.checkAndReuseBackingImageFileWithoutLock(); err == nil {
		log.Infof("Backing Image: succeeded to reuse the existing backing image file, will skip pulling")
		return bi.rpcResponse(), nil
	}
	log.Infof("Backing Image: failed to try to check or reuse the possible existing backing image file, will start pulling then: %v", err)

	if err := bi.prepareForFileHandling(); err != nil {
		return nil, errors.Wrapf(err, "failed to prepare for pulling")
	}

	size, err := bi.handler.GetSize(bi.URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get file size before pulling")
	}
	if size <= 0 {
		log.Warnf("Backing Image: cannot get size from URL, will set size after pulling")
	}
	bi.size = size

	go func() {
		defer func() {
			bi.updateCh <- nil
		}()

		if _, err := bi.handler.Pull(bi.URL, filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName), bi); err != nil {
			bi.lock.Lock()
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			log.WithError(err).Error("Backing Image: failed to pull from remote")
			bi.handler.Cancel()
			bi.lock.Unlock()
			return
		}
		bi.completeFileHandlingWIthLock()
		return
	}()
	go bi.waitForFileHandlingStartWithLock()

	log.Info("Backing Image: pulling backing image")

	return bi.rpcResponse(), nil
}

func (bi *BackingImage) Delete() (err error) {
	bi.lock.Lock()
	oldState := bi.state
	defer func() {
		currentState := bi.state
		bi.lock.Unlock()
		bi.handler.Cancel()
		if oldState != currentState {
			bi.updateCh <- nil
		}
	}()

	bi.log.Info("Backing Image: start to clean up backing image")

	if err := os.RemoveAll(bi.WorkDirectory); err != nil {
		err = errors.Wrapf(err, "failed to clean up work directory %v when deleting the backing image", bi.WorkDirectory)
		bi.state = StateFailed
		bi.errorMsg = err.Error()
		bi.log.WithError(err).Error("Backing Image: failed to do cleanup")
		return err
	}

	bi.log.Info("Backing Image: cleanup succeeded")

	return nil
}

func (bi *BackingImage) Get() (resp *rpc.BackingImageResponse) {
	var err error
	bi.lock.Lock()
	oldState := bi.state
	defer func() {
		if err != nil {
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			bi.handler.Cancel()
			bi.log.WithError(err).Error("Backing Image: failed to get backing image")
		}
		resp = bi.rpcResponse()
		currentState := bi.state
		bi.lock.Unlock()
		if oldState != currentState {
			bi.updateCh <- nil
		}
	}()

	if err = bi.validateFiles(); err != nil {
		return
	}

	if bi.state == types.BackingImageStateReady && bi.size <= 0 {
		err = fmt.Errorf("invalid size %v for ready file", bi.size)
		return
	}

	return
}

func (bi *BackingImage) Receive(size int64, senderManagerAddress string, portAllocateFunc func(portCount int32) (int32, int32, error), portReleaseFunc func(start, end int32) error) (port int32, err error) {
	bi.lock.Lock()
	log := bi.log
	log.Info("Backing Image: start to receive backing image")

	if bi.state != types.BackingImageStatePending {
		state := bi.state
		bi.lock.Unlock()
		return 0, fmt.Errorf("invalid state %v for receiving", state)
	}

	defer func() {
		if err != nil {
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			log.WithError(err).Error("Backing Image: failed to receive backing image")
			bi.handler.Cancel()
		}
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	// This means state was pending but somehow the handler had been initialized.
	if err := bi.handler.InitProcessing(); err != nil {
		return 0, errors.Wrapf(err, "failed to ask for the handler init before receiving")
	}
	bi.state = types.BackingImageStateStarting

	if err = bi.checkAndReuseBackingImageFileWithoutLock(); err == nil {
		log.Infof("Backing Image: succeeded to reuse the existing backing image file, will skip syncing")
		return 0, nil
	}
	log.Infof("Backing Image: failed to try to check or reuse the possible existing backing image file, will start syncing then: %v", err)

	if err := bi.prepareForFileHandling(); err != nil {
		return 0, errors.Wrapf(err, "failed to prepare for backing image receiving")
	}

	if port, _, err = portAllocateFunc(1); err != nil {
		return 0, errors.Wrapf(err, "failed to request a port for backing image receiving")
	}

	bi.size = size
	bi.senderManagerAddress = senderManagerAddress
	log = log.WithField("senderManagerAddress", senderManagerAddress)
	bi.log = log

	go func() {
		defer func() {
			bi.updateCh <- nil
			if err := portReleaseFunc(port, port+1); err != nil {
				log.WithError(err).Errorf("Failed to release port %v after receiving backing image", port)
			}
		}()

		log.Infof("Backing Image: prepare to receive backing image at port %v", port)

		if err := bi.handler.Receive(strconv.Itoa(int(port)), filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName), bi); err != nil {
			bi.lock.Lock()
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			log.WithError(err).Errorf("Backing Image: failed to receive backing image from %v", senderManagerAddress)
			bi.handler.Cancel()
			bi.lock.Unlock()
			return
		}
		bi.completeFileHandlingWIthLock()
		return
	}()
	go bi.waitForFileHandlingStartWithLock()

	return port, nil
}

func (bi *BackingImage) Send(address string, portAllocateFunc func(portCount int32) (int32, int32, error), portReleaseFunc func(start, end int32) error) (err error) {
	bi.lock.Lock()
	log := bi.log
	oldState := bi.state
	defer func() {
		currentState := bi.state
		bi.lock.Unlock()
		if oldState != currentState {
			bi.updateCh <- nil
		}
	}()

	if bi.state != types.BackingImageStateReady {
		return fmt.Errorf("backing image %v with state %v is invalid for file sending", bi.Name, bi.state)
	}
	if err := bi.validateFiles(); err != nil {
		bi.state = StateFailed
		bi.errorMsg = err.Error()
		log.WithError(err).Error("Backing Image: failed to validate files before sending")
		return errors.Wrapf(err, "cannot send backing image %v to others since the files are invalid", bi.Name)
	}
	if bi.sendingReference >= types.SendingLimit {
		return fmt.Errorf("backing image %v is already sending data to %v backing images", bi.Name, types.SendingLimit)
	}

	port, _, err := portAllocateFunc(1)
	if err != nil {
		return errors.Wrapf(err, "failed to request a port for backing image sending")
	}

	bi.sendingReference++

	go func() {
		log.Infof("Backing Image: start to send backing image to address %v", address)
		defer func() {
			bi.lock.Lock()
			bi.sendingReference--
			bi.lock.Unlock()
			bi.updateCh <- nil
			if err := portReleaseFunc(port, port+1); err != nil {
				log.WithError(err).Errorf("Failed to release port %v after sending backing image", port)
			}
		}()

		if err := bi.handler.Send(filepath.Join(bi.WorkDirectory, types.BackingImageFileName), address); err != nil {
			log.WithError(err).Errorf("Backing Image: failed to send backing image to address %v", address)
			return
		}
		log.Infof("Backing Image: done sending backing image to address %v", address)
	}()

	return nil
}

func (bi *BackingImage) LaunchUploadServer(portAllocateFunc func(portCount int32) (int32, int32, error), portReleaseFunc func(start, end int32) error) (port int32, err error) {
	bi.lock.Lock()
	log := bi.log
	log.Info("Backing Image: start to upload backing image")

	if bi.state != types.BackingImageStatePending {
		state := bi.state
		bi.lock.Unlock()
		return 0, fmt.Errorf("invalid state %v for uploading", state)
	}

	defer func() {
		if err != nil {
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			log.WithError(err).Error("Backing Image: failed to upload backing image")
			bi.handler.Cancel()
		}
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	// This means state was pending but somehow the handler had been initialized.
	if err := bi.handler.InitProcessing(); err != nil {
		return 0, errors.Wrapf(err, "failed to ask for the handler init before uploading")
	}
	bi.state = types.BackingImageStateStarting

	if err = bi.checkAndReuseBackingImageFileWithoutLock(); err == nil {
		log.Infof("Backing Image: succeeded to reuse the existing backing image file, will skip uploading")
		return 0, nil
	}
	log.Infof("Backing Image: failed to try to check or reuse the possible existing backing image file, will start uploading then: %v", err)

	if err := bi.prepareForFileHandling(); err != nil {
		return 0, errors.Wrapf(err, "failed to prepare for backing image uploading")
	}

	if port, _, err = portAllocateFunc(1); err != nil {
		return 0, errors.Wrapf(err, "failed to request a port for backing image uploading server launch")
	}
	bi.uploadPort = port

	go func() {
		defer func() {
			bi.updateCh <- nil
			if err := portReleaseFunc(port, port+1); err != nil {
				log.WithError(err).Errorf("Failed to release port %v after uploading backing image", port)
			}
		}()

		log.Infof("Backing Image: prepare to upload backing image at port %v", port)

		if err := bi.handler.Upload(strconv.Itoa(int(port)), bi.WorkDirectory, bi, bi); err != nil {
			bi.lock.Lock()
			bi.uploadPort = 0
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			log.WithError(err).Errorf("Backing Image: failed to upload backing image at port %v", port)
			bi.handler.Cancel()
			bi.lock.Unlock()
			return
		}
		bi.completeFileHandlingWIthLock()
		return
	}()
	// go bi.waitForFileHandlingStartWithLock()

	return port, nil
}

func (bi *BackingImage) rpcResponse() *rpc.BackingImageResponse {
	resp := &rpc.BackingImageResponse{
		Spec: &rpc.BackingImageSpec{
			Name:      bi.Name,
			Url:       bi.URL,
			Uuid:      bi.UUID,
			Size:      bi.size,
			Directory: bi.HostDirectory,
		},

		Status: &rpc.BackingImageStatus{
			State:                string(bi.state),
			SendingReference:     int32(bi.sendingReference),
			ErrorMsg:             bi.errorMsg,
			SenderManagerAddress: bi.senderManagerAddress,
			Progress:             int32(bi.progress),
			UploadPort:           bi.uploadPort,
		},
	}
	return resp
}

func (bi *BackingImage) checkAndReuseBackingImageFileWithoutLock() error {
	backingImagePath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)
	info, err := os.Stat(backingImagePath)
	if err != nil {
		return err
	}
	cfg, err := util.ReadBackingImageConfigFile(bi.WorkDirectory)
	if err != nil {
		return err
	}
	if info.Size() != cfg.Size || bi.Name != cfg.Name || bi.UUID != bi.UUID {
		return fmt.Errorf("backing image config %+v doesn't match the backing image current status or actual file size %v", cfg, info.Size())
	}

	bi.size = cfg.Size
	bi.processedSize = cfg.Size
	bi.progress = 100
	bi.state = types.BackingImageStateReady
	bi.handler.Cancel()
	bi.log.Infof("Backing Image: Directly reuse/introduce the existing file in path %v", backingImagePath)

	return nil
}

func (bi *BackingImage) prepareForFileHandling() error {
	mkdirRequired := false
	stat, err := os.Stat(bi.WorkDirectory)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		mkdirRequired = true
	} else {
		if !stat.IsDir() {
			if err := os.Remove(bi.WorkDirectory); err != nil {
				return errors.Wrapf(err, "failed to clean up the file occupying work directory name %v before file handling", bi.WorkDirectory)
			}
			mkdirRequired = true
		}
	}
	if mkdirRequired {
		if err := os.Mkdir(bi.WorkDirectory, 666); err != nil && !os.IsExist(err) {
			return errors.Wrapf(err, "failed to create work directory %v before file handling", bi.WorkDirectory)
		}
	}

	// Do not blindly remove all contents in the work directory in case of skipping
	// uploaded chunks
	backingImageCfgPath := filepath.Join(bi.WorkDirectory, util.BackingImageConfigFile)
	backingImageTmpPath := filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName)
	backingImagePath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)
	if err := os.Remove(backingImageCfgPath); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "failed to remove old backing image cfg file %v before file handling", backingImageCfgPath)
	}
	if err := os.Remove(backingImageTmpPath); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "failed to remove old backing image tmp file %v before file handling", backingImageTmpPath)
	}
	if err := os.Remove(backingImagePath); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "failed to remove old backing image file %v before file handling", backingImagePath)
	}
	return nil
}

func (bi *BackingImage) validateFiles() error {
	switch bi.state {
	case StateInProgress:
		backingImageTmpPath := filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName)
		if _, err := os.Stat(backingImageTmpPath); err != nil {
			return errors.Wrapf(err, "failed to validate backing image tmp file existence for handling backing image")
		}
		return nil
	case StateReady:
		backingImagePath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)
		if _, err := os.Stat(backingImagePath); err != nil {
			return errors.Wrapf(err, "failed to validate backing image file existence for ready backing image")
		}
		configFilePath := filepath.Join(bi.WorkDirectory, util.BackingImageConfigFile)
		if _, err := os.Stat(configFilePath); err != nil {
			return errors.Wrapf(err, "failed to validate backing image config file existence for ready backing image")
		}
	// Don't need to check anything for a failed/pending backing image.
	// Let's directly wait for cleanup then re-handling.
	case StatePending:
	case StateStarting:
	case StateFailed:
	default:
		return fmt.Errorf("unexpected state for file validation")
	}

	return nil
}

func (bi *BackingImage) waitForFileHandlingStartWithLock() {
	count := 0
	ticker := time.NewTicker(RetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			count++
			bi.lock.Lock()
			if bi.state != types.BackingImageStateStarting {
				bi.lock.Unlock()
				return
			}
			if count >= RetryCount {
				bi.state = types.BackingImageStateFailed
				bi.errorMsg = fmt.Sprintf("failed to wait for file handling start in %v seconds", RetryCount)
				bi.log.Errorf("Backing Image: %v", bi.errorMsg)
				bi.lock.Unlock()
				bi.handler.Cancel()
				bi.updateCh <- nil
				return
			}
			bi.lock.Unlock()
		}
	}
}

func (bi *BackingImage) completeFileHandlingWIthLock() {
	backingImageTmpPath := filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName)
	backingImagePath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)

	bi.lock.Lock()
	log := bi.log

	var err error
	defer func() {
		if err != nil {
			if bi.state != StateFailed {
				bi.state = StateFailed
				bi.errorMsg = err.Error()
				log.WithError(err).Error("Backing Image: failed to complete file handling")
				bi.handler.Cancel()
			}
		}
		bi.lock.Unlock()
	}()

	if bi.state != StateInProgress {
		log.Warnf("Backing Image: invalid state %v after file handling", bi.state)
		return
	}

	tmpFileStat, err := os.Stat(backingImageTmpPath)
	if err != nil {
		err = errors.Wrapf(err, "failed to check the tmp file after file handling")
		return
	}
	if tmpFileStat.Size() != bi.size {
		log.Debugf("Backing Image: update image size %v to the actual file size %v after file handling", bi.size, tmpFileStat.Size())
		bi.size = tmpFileStat.Size()
	}
	if tmpFileStat.Size() != bi.processedSize {
		log.Debugf("Backing Image: processed size %v is not equal to the actual file size %v after file handling", bi.processedSize, tmpFileStat.Size())
		bi.processedSize = tmpFileStat.Size()
	}

	if err := os.Rename(backingImageTmpPath, backingImagePath); err != nil {
		err = errors.Wrapf(err, "failed to rename backing image file after file handling")
		return
	}

	if err := util.WriteBackingImageConfigFile(bi.WorkDirectory, &util.BackingImageConfig{
		Name: bi.Name,
		UUID: bi.UUID,
		URL:  bi.URL,
		Size: bi.size,
	}); err != nil {
		err = errors.Wrapf(err, "failed to write backing image config file after file handling")
	}

	bi.progress = 100
	bi.uploadPort = 0
	bi.state = StateReady
	log.Infof("Backing Image: backing image file is ready")

	// Try to do cleanup after file handling complete
	files, err := ioutil.ReadDir(bi.WorkDirectory)
	if err != nil {
		log.WithError(err).Errorf("failed to list all files in the work directory, will give up cleanup after file handling complete")
	}
	for _, f := range files {
		fName := f.Name()
		if fName == util.BackingImageConfigFile || fName == types.BackingImageFileName {
			continue
		}
		if err := os.Remove(fName); err != nil && !os.IsNotExist(err) {
			log.WithError(err).Errorf("failed to remove redundant file %v after file handling complete", fName)
		}
	}

	return
}

func (bi *BackingImage) UpdateSyncFileProgress(size int64) {
	updateRequired := false
	defer func() {
		if updateRequired {
			bi.updateCh <- nil
		}
	}()

	bi.lock.Lock()
	defer bi.lock.Unlock()

	if bi.state == types.BackingImageStateStarting {
		bi.state = types.BackingImageStateInProgress
		updateRequired = true
	}

	oldProgress := bi.progress
	bi.processedSize = bi.processedSize + size
	if bi.size > 0 {
		bi.progress = int((float32(bi.processedSize) / float32(bi.size)) * 100)
	}
	if bi.progress != oldProgress {
		updateRequired = true
	}
}

func (bi *BackingImage) SetUploadSize(size int64) {
	bi.lock.Lock()
	defer bi.lock.Unlock()
	bi.size = size
}
