package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
	sparserest "github.com/longhorn/sparse-tools/sparse/rest"

	"github.com/longhorn/backing-image-manager/pkg/rpc"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

type state string

const (
	StatePending     = state(types.DownloadStatePending)
	StateDownloading = state(types.DownloadStateDownloading)
	StateDownloaded  = state(types.DownloadStateDownloaded)
	StateFailed      = state(types.DownloadStateFailed)
)

const (
	WaitRetryInterval = time.Second
	WaitRetryCount    = 30
)

type BackingImage struct {
	Name          string
	URL           string
	HostDirectory string
	WorkDirectory string
	state         state
	errorMsg      string

	sendingReference     int
	senderManagerAddress string

	// Need to acquire lock when access to BackingImage fields as well as its meta file.
	lock *sync.RWMutex

	log      logrus.FieldLogger
	updateCh chan interface{}
}

func NewBackingImage(name, url, diskPath string) *BackingImage {
	hostDir := filepath.Join(diskPath, types.BackingImageDirectoryName, name)
	workDir := filepath.Join(types.WorkDirectory, name)
	return &BackingImage{
		Name:          name,
		URL:           url,
		HostDirectory: hostDir,
		WorkDirectory: workDir,
		state:         StatePending,
		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component": "backing-image",
				"name":      name,
				"url":       url,
				"hostDir":   hostDir,
				"workDir":   workDir,
			},
		),
		lock: &sync.RWMutex{},
	}
}

func (bi *BackingImage) SetUpdateChannel(updateCh chan interface{}) {
	bi.updateCh = updateCh
}

func IntroduceDownloadedBackingImage(name, url, diskPath string) *BackingImage {
	bi := NewBackingImage(name, url, diskPath)
	bi.state = types.DownloadStateDownloaded
	return bi
}

func (bi *BackingImage) Pull() (resp *rpc.BackingImageResponse, err error) {
	bi.lock.Lock()
	defer func() {
		if err != nil {
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			bi.log.WithError(err).Error("Backing Image: failed to pull backing image")
		}
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()
	bi.log.Info("Backing Image: start to pull backing image")

	if err := bi.prepareForDownload(); err != nil {
		return nil, errors.Wrapf(err, "failed to prepare for pulling")
	}

	go func() {
		defer func() {
			bi.updateCh <- nil
		}()

		if err := util.DownloadFile(filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName), bi.URL); err != nil {
			bi.lock.Lock()
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			bi.log.WithError(err).Error("Backing Image: failed to pull from remote")
			bi.lock.Unlock()
			return
		}
		bi.renameFileAndUpdateWithLockAfterDownloadComplete()
		return
	}()

	go bi.waitForFileAndUpdateWithLockWhenDownloadStart()

	bi.log.Info("Backing Image: pulling backing image")

	return bi.rpcResponse(), nil
}

func (bi *BackingImage) Delete() (err error) {
	bi.lock.Lock()
	oldState := bi.state
	defer func() {
		currentState := bi.state
		bi.lock.Unlock()
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

func (bi *BackingImage) Get() (*rpc.BackingImageResponse, error) {
	bi.lock.Lock()
	oldState := bi.state
	defer func() {
		currentState := bi.state
		bi.lock.Unlock()
		if oldState != currentState {
			bi.updateCh <- nil
		}
	}()

	if err := bi.validateFiles(); err != nil {
		bi.state = StateFailed
		bi.errorMsg = err.Error()
		bi.log.WithError(err).Error("Backing Image: failed to validate files when getting backing image")
		return nil, errors.Wrapf(err, "failed to validate files when getting backing image %v", bi.Name)
	}

	return bi.rpcResponse(), nil
}

func (bi *BackingImage) Receive(port int32, senderManagerAddress string, portReleaseFunc func()) (resp *rpc.BackingImageResponse, err error) {
	bi.lock.Lock()
	defer func() {
		if err != nil {
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			bi.log.WithError(err).Error("Backing Image: failed to receive backing image")
		}
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	bi.senderManagerAddress = senderManagerAddress
	bi.log = bi.log.WithField("senderManagerAddress", senderManagerAddress)
	bi.log.Infof("Backing Image: prepare to receive backing image at port %v", port)

	if err := bi.prepareForDownload(); err != nil {
		return nil, errors.Wrapf(err, "failed to prepare for backing image receiving")
	}

	go func() {
		defer func() {
			bi.updateCh <- nil
			portReleaseFunc()
		}()

		if err := sparserest.Server(strconv.Itoa(int(port)), filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName), &sparserest.SyncFileStub{}); err != nil && err != http.ErrServerClosed {
			bi.lock.Lock()
			bi.state = StateFailed
			bi.errorMsg = err.Error()
			bi.log.WithError(err).Errorf("Backing Image: failed to receive backing image from %v", senderManagerAddress)
			bi.lock.Unlock()
			return
		}
		bi.renameFileAndUpdateWithLockAfterDownloadComplete()
		return
	}()

	go bi.waitForFileAndUpdateWithLockWhenDownloadStart()

	bi.log.Infof("Backing image: receiving backing image from %v", senderManagerAddress)

	return bi.rpcResponse(), nil
}

func (bi *BackingImage) Send(address string, portReleaseFunc func()) error {
	bi.lock.Lock()
	oldState := bi.state
	defer func() {
		currentState := bi.state
		bi.lock.Unlock()
		if oldState != currentState {
			bi.updateCh <- nil
		}
	}()

	if bi.state != types.DownloadStateDownloaded {
		return fmt.Errorf("backing image %v with state %v is invalid for file sending", bi.Name, bi.state)
	}
	if err := bi.validateFiles(); err != nil {
		bi.state = StateFailed
		bi.errorMsg = err.Error()
		bi.log.WithError(err).Error("Backing Image: failed to validate files before sending")
		return errors.Wrapf(err, "cannot send backing image %v to others since the files are invalid", bi.Name)
	}
	if bi.sendingReference >= types.SendingLimit {
		return fmt.Errorf("backing image %v is already sending data to %v backing images", bi.Name, types.SendingLimit)
	}
	bi.sendingReference++

	bi.log.Infof("Backing Image: start to send backing image %v to %v", bi.Name, address)
	backingFilepath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)
	go func() {
		defer portReleaseFunc()

		if err := sparse.SyncFile(backingFilepath, address, types.FileSyncTimeout, false); err != nil {
			bi.log.WithError(err).Errorf("Backing Image: failed to send backing image to address %v", address)
			return
		}
		bi.lock.Lock()
		bi.sendingReference--
		bi.log.Infof("Backing Image: done sending backing image to address %v", bi.Name, address)
		bi.lock.Unlock()
		bi.updateCh <- nil
	}()

	bi.log.Infof("Backing image: sending backing image")

	return nil
}

func (bi *BackingImage) rpcResponse() *rpc.BackingImageResponse {
	resp := &rpc.BackingImageResponse{
		Spec: &rpc.BackingImageSpec{
			Name:      bi.Name,
			Url:       bi.URL,
			Directory: bi.HostDirectory,
		},

		Status: &rpc.BackingImageStatus{
			State:                string(bi.state),
			SendingReference:     int32(bi.sendingReference),
			ErrorMsg:             bi.errorMsg,
			SenderManagerAddress: bi.senderManagerAddress,
		},
	}
	return resp
}

func (bi *BackingImage) prepareForDownload() error {
	if err := os.RemoveAll(bi.WorkDirectory); err != nil {
		return errors.Wrapf(err, "failed to clean up work directory %v before downloading", bi.WorkDirectory)
	}
	if err := os.Mkdir(bi.WorkDirectory, 666); err != nil {
		return errors.Wrapf(err, "failed to create work directory %v before downloading", bi.WorkDirectory)
	}
	return nil
}

func (bi *BackingImage) validateFiles() error {
	switch bi.state {
	case StateDownloading:
		backingImageTmpPath := filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName)
		if _, err := os.Stat(backingImageTmpPath); err != nil {
			return errors.Wrapf(err, "failed to validate backing image tmp file existence for downloading backing image")
		}
		return nil
	case StateDownloaded:
		backingImagePath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)
		if _, err := os.Stat(backingImagePath); err != nil {
			return errors.Wrapf(err, "failed to validate backing image file existence for downloaded backing image")
		}
	// Don't need to check anything for a failed/pending backing image.
	// Let's directly wait for cleanup then re-downloading.
	case StatePending:
	case StateFailed:
	default:
		return fmt.Errorf("unexpected state for file validation")
	}

	return nil
}

func (bi *BackingImage) waitForFileAndUpdateWithLockWhenDownloadStart() {
	defer func() {
		bi.updateCh <- nil
	}()

	backingImageTmpPath := filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName)
	for count := 0; count < WaitRetryCount; count++ {
		bi.lock.Lock()
		if bi.state != types.DownloadStatePending {
			bi.lock.Unlock()
			return
		}
		_, err := os.Stat(backingImageTmpPath)
		if err == nil {
			bi.state = StateDownloading
			bi.lock.Unlock()
			return
		}
		if !os.IsNotExist(err) {
			bi.state = StateFailed
			bi.errorMsg = errors.Wrapf(err, "failed to check downloading file for pending backing image").Error()
			bi.log.WithError(err).Error("Backing Image: failed to check downloading file for pending backing image")
			bi.lock.Unlock()
			return
		}
		bi.lock.Unlock()
		time.Sleep(WaitRetryInterval)
	}

	bi.lock.Lock()
	defer bi.lock.Unlock()
	if bi.state == types.DownloadStatePending {
		bi.state = StateFailed
		bi.errorMsg = fmt.Sprintf("%v second download wait time limit exceeded", WaitRetryCount)
		bi.log.Errorf("Backing Image: %v second download wait time limit exceeded", WaitRetryCount)
	}
	return
}

func (bi *BackingImage) renameFileAndUpdateWithLockAfterDownloadComplete() {
	backingImageTmpPath := filepath.Join(bi.WorkDirectory, types.BackingImageTmpFileName)
	backingImagePath := filepath.Join(bi.WorkDirectory, types.BackingImageFileName)

	bi.lock.Lock()
	defer bi.lock.Unlock()

	if bi.state == StateFailed {
		bi.log.Warnf("Backing Image: state somehow becomes %v after downloading, will not continue renaming file", types.DownloadStateFailed)
		return
	}

	if err := os.Rename(backingImageTmpPath, backingImagePath); err != nil {
		bi.state = StateFailed
		bi.errorMsg = errors.Wrapf(err, "failed to rename backing image file after downloading").Error()
		bi.log.WithError(err).Error("Backing Image: failed to rename backing image file after downloading")
		return
	}
	bi.state = StateDownloaded
	bi.log.Infof("Backing Image: downloaded backing image file")
	return
}
