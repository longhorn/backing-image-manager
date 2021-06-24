package server

import (
	"fmt"
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

const (
	RetryInterval = 1 * time.Second
	RetryCount    = 30
)

type BackingImage struct {
	Name             string
	UUID             string
	WorkDirectory    string
	DiskPath         string
	state            types.State
	expectedChecksum string
	currentChecksum  string
	errorMsg         string

	size          int64
	processedSize int64
	progress      int

	sendingReference     int
	senderManagerAddress string

	// Need to acquire lock when access to BackingImage fields as well as its meta file.
	lock *sync.RWMutex

	log      logrus.FieldLogger
	updateCh chan interface{}

	handler *BackingImageHandler
}

func NewBackingImage(name, uuid, checksum, diskPath string, size int64, handler *BackingImageHandler, updateCh chan interface{}) *BackingImage {
	workDir := filepath.Join(diskPath, types.BackingImageManagerDirectoryName, GetBackingImageDirectoryName(name, uuid))
	return &BackingImage{
		Name:             name,
		UUID:             uuid,
		WorkDirectory:    workDir,
		DiskPath:         diskPath,
		state:            types.StatePending,
		size:             size,
		expectedChecksum: checksum,
		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component":        "backing-image",
				"name":             name,
				"uuid":             uuid,
				"size":             size,
				"workDir":          workDir,
				"expectedChecksum": checksum,
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

func (bi *BackingImage) Delete() (err error) {
	bi.lock.Lock()
	defer func() {
		if err != nil {
			bi.handleFailureWithoutLock(
				errors.Wrapf(err, "failed to clean up work directory %v when deleting the backing image", bi.WorkDirectory))
		}
		bi.handler.Cancel()
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	bi.log.Info("Backing Image: start to clean up backing image")

	if err := os.RemoveAll(bi.WorkDirectory); err != nil {
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
			bi.handleFailureWithoutLock(err)
		}
		if bi.state != oldState {
			bi.updateCh <- nil
		}
		resp = bi.rpcResponse()
		bi.lock.Unlock()
	}()

	if err = bi.validateFiles(); err != nil {
		return
	}

	if bi.state == types.StateReady && bi.size <= 0 {
		err = fmt.Errorf("invalid size %v for ready file", bi.size)
		return
	}

	return
}

func (bi *BackingImage) Receive(senderManagerAddress string, portAllocateFunc func(portCount int32) (int32, int32, error), portReleaseFunc func(start, end int32) error) (port int32, err error) {
	bi.lock.Lock()
	log := bi.log
	log.Info("Backing Image: start to receive backing image")

	defer func() {
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	if bi.state != types.StatePending {
		return 0, fmt.Errorf("invalid state %v for receiving", bi.state)
	}
	bi.state = types.StateStarting

	defer func() {
		if err != nil {
			bi.handleFailureWithoutLock(err)
		}
	}()

	// Try to check if the file already exists first
	if err = bi.checkAndReuseBackingImageFileWithoutLock(); err == nil {
		log.Infof("Backing Image: succeeded to reuse the existing backing image file, will skip syncing")
		return 0, nil
	}
	log.Infof("Backing Image: no existing backing image file for reusage, will start syncing then: %v", err)
	if err = bi.prepareWorkDirectory(); err != nil {
		return 0, errors.Wrapf(err, "failed to prepare for backing image receiving")
	}

	// This means state was pending but somehow the handler had been initialized.
	if err = bi.handler.InitProcessing(); err != nil {
		return 0, errors.Wrapf(err, "failed to ask for the handler init before receiving")
	}

	if port, _, err = portAllocateFunc(1); err != nil {
		return 0, errors.Wrapf(err, "failed to request a port for backing image receiving")
	}

	bi.senderManagerAddress = senderManagerAddress
	log = log.WithField("senderManagerAddress", senderManagerAddress)
	bi.log = log

	go func() {
		defer func() {
			if err := portReleaseFunc(port, port+1); err != nil {
				log.WithError(err).Errorf("Failed to release port %v after receiving backing image", port)
			}
		}()

		log.Infof("Backing Image: prepare to receive backing image at port %v", port)

		err := bi.handler.Receive(strconv.Itoa(int(port)), filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName), bi)
		bi.lock.Lock()
		bi.finishFileProcessingWithoutLock(err)
		bi.lock.Unlock()
		return
	}()
	go bi.waitForProcessingStartWithLock()

	return port, nil
}

func (bi *BackingImage) Send(address string, portAllocateFunc func(portCount int32) (int32, int32, error), portReleaseFunc func(start, end int32) error) (err error) {
	bi.lock.Lock()
	log := bi.log
	defer func() {
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	if bi.state != types.StateReady {
		return fmt.Errorf("backing image %v with state %v is invalid for file sending", bi.Name, bi.state)
	}
	if err := bi.validateFiles(); err != nil {
		err = errors.Wrapf(err, "cannot send backing image %v to others since the files are invalid", bi.Name)
		bi.handleFailureWithoutLock(err)
		return err
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

func (bi *BackingImage) Fetch(sourceFileName string) (err error) {
	bi.lock.Lock()
	log := bi.log
	log.Info("Backing Image: start to fetch backing image")

	defer func() {
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	if bi.state != types.StatePending {
		return fmt.Errorf("invalid state %v for fetching", bi.state)
	}
	bi.state = types.StateStarting

	defer func() {
		if err != nil {
			bi.handleFailureWithoutLock(err)
		}
	}()

	// Try to check if the file already exists first
	sourceFilePath := filepath.Join(bi.DiskPath, types.DataSourceDirectoryName, sourceFileName)
	if err = bi.checkAndReuseBackingImageFileWithoutLock(); err == nil {
		log.Infof("Backing Image: succeeded to reuse the existing backing image file", sourceFilePath)
		if sourceFileName != "" {
			log.Infof("Backing Image: start to clean up the source file %v and skip fetching", sourceFilePath)
			if err := os.RemoveAll(sourceFilePath); err != nil {
				log.Errorf("Backing Image: failed to clean up the source file after skipping fetching: %v", err)
			}
		}
		return nil
	}
	// If the source file name is empty, it means the caller is trying to reuse existing file only.
	if sourceFileName == "" {
		return errors.Wrapf(err, "failed to reuse file via the fetch call")
	}
	log.Infof("Backing Image: no existing backing image file for reusage, will start fetching from %v then: %v", sourceFilePath, err)

	if err = bi.prepareWorkDirectory(); err != nil {
		return errors.Wrapf(err, "failed to prepare for backing image fetching")
	}

	bi.log = log

	err = os.Rename(sourceFilePath, filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName))
	bi.state = types.StateInProgress
	bi.finishFileProcessingWithoutLock(err)

	return nil
}

func (bi *BackingImage) rpcResponse() *rpc.BackingImageResponse {
	resp := &rpc.BackingImageResponse{
		Spec: &rpc.BackingImageSpec{
			Name:     bi.Name,
			Uuid:     bi.UUID,
			Size:     bi.size,
			Checksum: bi.expectedChecksum,
		},

		Status: &rpc.BackingImageStatus{
			State:                string(bi.state),
			SendingReference:     int32(bi.sendingReference),
			ErrorMsg:             bi.errorMsg,
			SenderManagerAddress: bi.senderManagerAddress,
			Progress:             int32(bi.progress),
			Checksum:             bi.currentChecksum,
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
	if bi.size > 0 && bi.size != info.Size() {
		return fmt.Errorf("backing image expected size %v doesn't match the existing file size %v", bi.size, info.Size())
	}

	checksum, err := util.GetFileChecksum(backingImagePath)
	if err != nil {
		return errors.Wrapf(err, "failed to calculate checksum for the tmp file after processing")
	}
	bi.currentChecksum = checksum

	if bi.expectedChecksum != "" && bi.expectedChecksum != bi.currentChecksum {
		return fmt.Errorf("backing image expected checksum %v doesn't match the existing file checksum %v", bi.expectedChecksum, bi.currentChecksum)
	}

	bi.size = info.Size()
	bi.processedSize = bi.size
	bi.progress = 100
	bi.state = types.StateReady
	bi.handler.Cancel()
	bi.log = bi.log.WithFields(logrus.Fields{
		"size":            bi.size,
		"currentChecksum": bi.currentChecksum,
	})

	// Do not rely on the values in the config file to verify the backing image.
	// The source of truth should be the file itself.
	// The config file is used to record the meta info only.
	if err := util.WriteBackingImageConfigFile(bi.WorkDirectory, &util.BackingImageConfig{
		Name:             bi.Name,
		UUID:             bi.UUID,
		Size:             bi.size,
		ExpectedChecksum: bi.expectedChecksum,
		CurrentChecksum:  bi.currentChecksum,
	}); err != nil {
		bi.log.Warnf("Backing Image: failed to update backing image config file when reusing existing file: %v", err)
	}

	bi.log.Infof("Backing Image: Directly reuse/introduce the existing file in path %v", backingImagePath)

	return nil
}

func (bi *BackingImage) prepareWorkDirectory() error {
	if err := os.RemoveAll(bi.WorkDirectory); err != nil {
		return errors.Wrapf(err, "failed to clean up the work directory %v before processing backing image", bi.WorkDirectory)
	}
	if err := os.Mkdir(bi.WorkDirectory, 666); err != nil && !os.IsExist(err) {
		return errors.Wrapf(err, "failed to create work directory %v before processing backing image", bi.WorkDirectory)
	}
	return nil
}

func (bi *BackingImage) validateFiles() error {
	switch bi.state {
	case types.StateInProgress:
		backingImageTmpPath := filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName)
		if _, err := os.Stat(backingImageTmpPath); err != nil {
			return errors.Wrapf(err, "failed to validate backing image tmp file existence for in-progress backing image")
		}
		return nil
	case types.StateReady:
		backingImagePath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)
		if _, err := os.Stat(backingImagePath); err != nil {
			return errors.Wrapf(err, "failed to validate backing image file existence for ready backing image")
		}
		configFilePath := filepath.Join(bi.WorkDirectory, util.BackingImageConfigFile)
		if _, err := os.Stat(configFilePath); err != nil {
			return errors.Wrapf(err, "failed to validate backing image config file existence for ready backing image")
		}
	// Don't need to check anything for a failed/pending backing image.
	// Let's directly wait for cleanup then re-processing.
	case types.StatePending:
	case types.StateStarting:
	case types.StateFailed:
	default:
		return fmt.Errorf("unexpected state for file validation")
	}

	return nil
}

func (bi *BackingImage) handleFailureWithoutLock(err error) {
	if err == nil {
		return
	}
	if bi.state == types.StateFailed {
		return
	}
	bi.state = types.StateFailed
	bi.errorMsg = fmt.Sprintf("failed to process backing image file: %v", err)
	bi.log.Errorf("Backing Image: %s", bi.errorMsg)
}

func (bi *BackingImage) waitForProcessingStartWithLock() {
	count := 0
	ticker := time.NewTicker(RetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			count++
			bi.lock.Lock()
			if bi.state != types.StateStarting {
				bi.lock.Unlock()
				return
			}
			if count >= RetryCount {
				bi.handleFailureWithoutLock(fmt.Errorf("failed to wait for processing start in %v seconds", RetryCount))
				bi.lock.Unlock()
				bi.updateCh <- nil
				return
			}
			bi.lock.Unlock()
		}
	}
}

func (bi *BackingImage) finishFileProcessingWithoutLock(err error) {
	backingImageTmpPath := filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName)
	backingImagePath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)

	log := bi.log

	defer func() {
		if err != nil {
			bi.handleFailureWithoutLock(err)
		}
		bi.updateCh <- nil
	}()

	if err != nil {
		return
	}

	if bi.state != types.StateInProgress {
		log.Warnf("Backing Image: invalid state %v after processing", bi.state)
		return
	}

	tmpFileStat, err := os.Stat(backingImageTmpPath)
	if err != nil {
		err = errors.Wrapf(err, "failed to check the tmp file after processing")
		return
	}
	if tmpFileStat.Size() != bi.size {
		log.Debugf("Backing Image: update image size %v to the actual file size %v after processing", bi.size, tmpFileStat.Size())
		bi.size = tmpFileStat.Size()
	}
	if tmpFileStat.Size() != bi.processedSize {
		log.Debugf("Backing Image: processed size %v is not equal to the actual file size %v after processing", bi.processedSize, tmpFileStat.Size())
		bi.processedSize = tmpFileStat.Size()
	}

	checksum, err := util.GetFileChecksum(backingImageTmpPath)
	if err != nil {
		err = errors.Wrapf(err, "failed to calculate checksum for the tmp file after processing")
		return
	}
	bi.currentChecksum = checksum
	if bi.expectedChecksum != "" && bi.expectedChecksum != bi.currentChecksum {
		err = fmt.Errorf("backing image expected checksum %v doesn't match the processed file checksum %v", bi.expectedChecksum, bi.currentChecksum)
		return
	}

	if err := os.Rename(backingImageTmpPath, backingImagePath); err != nil {
		err = errors.Wrapf(err, "failed to rename backing image file after processing")
		return
	}

	bi.progress = 100
	bi.state = types.StateReady
	bi.log = bi.log.WithFields(logrus.Fields{
		"size":            bi.size,
		"currentChecksum": bi.currentChecksum,
	})
	log = bi.log

	if err := util.WriteBackingImageConfigFile(bi.WorkDirectory, &util.BackingImageConfig{
		Name:             bi.Name,
		UUID:             bi.UUID,
		Size:             bi.size,
		ExpectedChecksum: bi.expectedChecksum,
		CurrentChecksum:  bi.currentChecksum,
	}); err != nil {
		log.Warnf("Backing Image: Failed to write config file after processing: %v", err)
	}

	log.Infof("Backing Image: backing image file is ready")
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

	if bi.state == types.StateStarting {
		bi.state = types.StateInProgress
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
