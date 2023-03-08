package sync

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	sparserest "github.com/longhorn/sparse-tools/sparse/rest"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

// SyncFile state machine:
//
//     +---------+  Try to reuse file (best effort),              +-----------+
//  +--+ pending +----------------------------------------------->+ starting  +-----+
//  |  +----+----+  do cleanup & preparation if reuse failed.     +-----+-----+     |
//  |       |                                                           |           |
//  |       |                                                           |           |
//  |       |                           Sender doesn't start sending    |           |
//  |       |                         +---------------------------------+           |
//  |       |                         | data after an interval.                     |
//  |       |                         |                                             |
//  |       |                         |                               Start syncing |
//  |       |                         |                               the file.     |
//  |       |                         v                                             |
//  |       |                  +------+------+                                      |
//  |       +----------------->+   failed    +<--------------------------+          |
//  |        Fail at cleanup   +------+------+                           |          |
//  |        or preparation.          ^                                  |          |
//  |                                 |           Fail at file handling, |          |
//  |  Reuse success.                 |                  data transport, |          |
//  |                                 |         or the final validation. |          |
//  |                                 |                                  |          |
//  |   +-------------+               |                                  |          |
//  |   +   unknown   + --------------+                                  |          |
//  |   +------+------+  Checksum is changed after modification.         |          |
//  |          ^                                                         |          |
//  |          | File is suddenly modified.                              |          |
//  |          | Need to re-calculate the checksum.                      |          |
//  |          v                                                         |          |
//  |   +-------------+                                           +------+------+   |
//  +-->+    ready    + <-----------------------------------------+ in-progress +<--+
//      +------+------+                                           +------+------+
//

const (
	RetryInterval   = 1 * time.Second
	RetryCount      = 60
	LargeRetryCount = 10 * 60

	TmpFileSuffix = ".tmp"
)

type SyncingFile struct {
	lock *sync.RWMutex

	log logrus.FieldLogger

	ctx    context.Context
	cancel context.CancelFunc

	filePath         string
	tmpFilePath      string
	uuid             string
	diskUUID         string
	size             int64
	state            types.State
	progress         int
	processedSize    int64
	expectedChecksum string
	currentChecksum  string
	modificationTime string
	message          string

	sendingReference int

	// for unit test
	handler Handler
}

func (sf *SyncingFile) UpdateProgress(processedSize int64) {
	sf.updateProgress(processedSize)
}

func (sf *SyncingFile) UpdateSyncFileProgress(size int64) {
	sf.updateProgress(size)
}

func (sf *SyncingFile) updateProgress(processedSize int64) {
	sf.lock.Lock()
	defer sf.lock.Unlock()

	if sf.state == types.StateStarting {
		sf.state = types.StateInProgress
	}
	if sf.state == types.StateReady {
		return
	}

	sf.processedSize = sf.processedSize + processedSize
	if sf.size > 0 {
		sf.progress = int((float32(sf.processedSize) / float32(sf.size)) * 100)
	}
}

func NewSyncingFile(parentCtx context.Context, filePath, uuid, diskUUID, expectedChecksum string, size int64, handler Handler) *SyncingFile {
	ctx, cancel := context.WithCancel(parentCtx)
	sf := &SyncingFile{
		lock: &sync.RWMutex{},
		log: logrus.StandardLogger().WithFields(
			logrus.Fields{
				"component": "sync-file",
				"filePath":  filePath,
				"uuid":      uuid,
			},
		),
		ctx:    ctx,
		cancel: cancel,

		filePath:         filePath,
		tmpFilePath:      fmt.Sprintf("%s%s", filePath, TmpFileSuffix),
		uuid:             uuid,
		diskUUID:         diskUUID,
		size:             size,
		expectedChecksum: expectedChecksum,

		state: types.StatePending,

		handler: handler,
	}
	if size > 0 {
		sf.log.WithField("size", size)
	}

	go func() {
		// This may be time-consuming.
		reuseErr := sf.checkAndReuseFile()
		if reuseErr == nil {
			return
		}
		sf.log.Infof("SyncingFile: cannot to reuse the existing file, will clean it up then start processing: %v", reuseErr)

		var err error
		defer func() {
			sf.lock.Lock()
			defer sf.lock.Unlock()
			if err != nil {
				sf.state = types.StateFailed
				sf.log.Infof("SyncingFile: failed to init file syncing: %v", err)
			} else {
				sf.state = types.StateStarting
				go sf.waitForProcessingBeginWithTimeout()
			}
		}()

		dir := filepath.Dir(sf.filePath)
		if mkErr := os.MkdirAll(dir, 0666); mkErr != nil && !os.IsExist(mkErr) {
			err = mkErr
			return
		}
		if err = os.RemoveAll(sf.tmpFilePath); err != nil {
			return
		}
		if err = os.RemoveAll(sf.filePath); err != nil {
			return
		}
	}()

	return sf
}

func (sf *SyncingFile) checkAndReuseFile() (err error) {
	sf.lock.RLock()
	expectedChecksum := sf.expectedChecksum
	filePath := sf.filePath
	size := sf.size
	sf.lock.RUnlock()

	configFilePath := util.GetSyncingFileConfigFilePath(filePath)
	defer func() {
		if err == nil {
			return
		}
		if rmErr := os.RemoveAll(configFilePath); rmErr != nil {
			sf.log.Warnf("SyncingFile: failed to clean up config file %v after reuse failure: %v", configFilePath, rmErr)
		}
	}()

	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if size > 0 && size != info.Size() {
		return fmt.Errorf("sync file expected size %v doesn't match the existing file size %v", size, info.Size())
	}

	var currentChecksum string
	config, err := util.ReadSyncingFileConfig(configFilePath)
	if config != nil && config.ModificationTime == info.ModTime().UTC().String() {
		logrus.Debugf("SyncingFile: directly get the checksum from a valid config during file reusage: %v", config.CurrentChecksum)
		currentChecksum = config.CurrentChecksum
	} else {
		logrus.Debugf("SyncingFile: failed to get the checksum from a valid config during file reusage, will directly calculated it then")
		currentChecksum, err = util.GetFileChecksum(filePath)
		if err != nil {
			return errors.Wrapf(err, "failed to calculate checksum for the existing file during init")
		}
	}
	if expectedChecksum != "" && expectedChecksum != currentChecksum {
		return fmt.Errorf("file expected checksum %v doesn't match the existing file checksum %v", expectedChecksum, currentChecksum)
	}

	sf.lock.Lock()
	sf.cancel()
	sf.currentChecksum = currentChecksum
	sf.processedSize = info.Size()
	sf.modificationTime = info.ModTime().UTC().String()
	sf.updateSyncReadyNoLock()
	sf.writeConfigNoLock()
	sf.lock.Unlock()

	sf.log.Infof("SyncingFile: directly reuse/introduce the existing file in path %v", filePath)

	return nil
}

func (sf *SyncingFile) waitForProcessingBeginWithTimeout() {
	count := 0
	ticker := time.NewTicker(RetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			count++
			sf.lock.Lock()
			notBeginYet := sf.state == "" || sf.state == types.StatePending || sf.state == types.StateStarting
			if !notBeginYet {
				sf.lock.Unlock()
				return
			}
			if count >= RetryCount {
				sf.handleFailureNoLock(fmt.Errorf("failed to wait for processing begin in %v seconds, current state %v", RetryCount, sf.state))
				sf.lock.Unlock()
				return
			}
			sf.lock.Unlock()
		}
	}
}

func (sf *SyncingFile) WaitForStateNonPending() error {
	count := 0
	ticker := time.NewTicker(RetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			count++
			sf.lock.RLock()
			state := sf.state
			sf.lock.RUnlock()
			if state != types.StatePending {
				return nil
			}
			if count >= LargeRetryCount {
				return fmt.Errorf("sync file is still in empty state after %v second", LargeRetryCount)
			}
		}
	}
}

func (sf *SyncingFile) Get() api.FileInfo {
	sf.lock.Lock()
	defer sf.lock.Unlock()

	// make sure the file data is intact when the state is ready
	if sf.state == types.StateReady {
		sf.validateReadyFileNoLock()
	}

	return sf.getNoLock()
}

func (sf *SyncingFile) validateReadyFileNoLock() {
	if sf.state != types.StateReady {
		return
	}
	if modificationTime := util.FileModificationTime(sf.filePath); modificationTime != sf.modificationTime {
		sf.state = types.StateUnknown
		sf.modificationTime = modificationTime
		sf.message = fmt.Sprintf("ready file is suddenly modified at %v, need to re-check the checksum", modificationTime)

		go func() {
			var err error
			if checksum, cksumErr := util.GetFileChecksum(sf.filePath); cksumErr != nil {
				err = errors.Wrapf(cksumErr, "failed to re-calculate checksum after ready file being modified, will mark the file as failed")
			} else if checksum != sf.currentChecksum {
				err = fmt.Errorf("ready file is modified at %v with a different checksum %v, previous checksum %v", modificationTime, checksum, sf.currentChecksum)
			}

			sf.lock.Lock()
			defer sf.lock.Unlock()
			if err != nil {
				sf.state = types.StateFailed
				sf.message = err.Error()
			} else {
				sf.state = types.StateReady
				sf.message = ""
				sf.writeConfigNoLock()
			}
		}()
	}
}

func (sf *SyncingFile) getNoLock() api.FileInfo {
	return api.FileInfo{
		DiskUUID:         sf.diskUUID,
		ExpectedChecksum: sf.expectedChecksum,

		FilePath:         sf.filePath,
		UUID:             sf.uuid,
		Size:             sf.size,
		State:            string(sf.state),
		Progress:         sf.progress,
		ProcessedSize:    sf.processedSize,
		CurrentChecksum:  sf.currentChecksum,
		ModificationTime: sf.modificationTime,
		Message:          sf.message,

		SendingReference: sf.sendingReference,
	}
}

func (sf *SyncingFile) Delete() {
	sf.log.Infof("SyncingFile: start to delete sync file")

	sf.lock.RLock()
	defer sf.lock.RUnlock()

	sf.cancel()
	if err := os.RemoveAll(sf.tmpFilePath); err != nil {
		sf.log.Warnf("SyncingFile: failed to delete tmp sync file %v: %v", sf.tmpFilePath, err)
	}
	if err := os.RemoveAll(sf.filePath); err != nil {
		sf.log.Warnf("SyncingFile: failed to delete sync file %v: %v", sf.filePath, err)
	}
	configFilePath := util.GetSyncingFileConfigFilePath(sf.filePath)
	if err := os.RemoveAll(configFilePath); err != nil {
		sf.log.Warnf("SyncingFile: failed to delete sync file config file %v: %v", configFilePath, err)
	}
	return
}

func (sf *SyncingFile) GetFileReader() (io.ReadCloser, error) {
	sf.log.Infof("SyncingFile: prepare a reader for the sync file")

	sf.lock.RLock()
	if sf.state != types.StateReady {
		sf.lock.RUnlock()
		return nil, fmt.Errorf("cannot get the reader for a non-ready file, current state %v", sf.state)
	}
	sf.lock.RUnlock()

	return os.Open(sf.filePath)
}

func (sf *SyncingFile) stateCheckBeforeProcessing() (bool, error) {
	sf.lock.RLock()
	defer sf.lock.RUnlock()

	if sf.state == types.StateReady {
		sf.log.Infof("SyncingFile: file is already state %v, no need to process it", types.StateReady)
		return false, nil
	}
	if sf.state != types.StateStarting {
		return false, fmt.Errorf("invalid state %v for actual processing", sf.state)
	}

	return true, nil
}

func (sf *SyncingFile) Fetch(srcFilePath string) (err error) {
	sf.log.Infof("SyncingFile: start to fetch sync file from %v", srcFilePath)

	shouldReuseFile := srcFilePath == sf.filePath

	srcConfigFilePath := util.GetSyncingFileConfigFilePath(srcFilePath)
	defer func() {
		// cleanup the src file
		if !shouldReuseFile {
			sf.log.Infof("SyncingFile: try to clean up src file %v and its config file %v after fetch", srcFilePath, srcConfigFilePath)
			if err := os.RemoveAll(srcFilePath); err != nil {
				sf.log.Errorf("SyncingFile: failed to clean up src file %v after fetch: %v", srcFilePath, err)
			}
			if err := os.RemoveAll(srcConfigFilePath); err != nil {
				sf.log.Errorf("SyncingFile: failed to clean up src config file %v after fetch: %v", srcConfigFilePath, err)
			}
		}
	}()

	needProcessing, err := sf.stateCheckBeforeProcessing()
	if err != nil {
		return err
	}
	if !needProcessing {
		return nil
	}

	defer func() {
		if finalErr := sf.finishProcessing(err); finalErr != nil {
			err = finalErr
		}
	}()

	if shouldReuseFile {
		return fmt.Errorf("syncing file should be directly reused but failed")
	}
	sf.log.Debugf("SyncingFile: need to fetch the syncing file from %v", srcFilePath)

	srcFileStat, err := os.Stat(srcFilePath)
	if err != nil {
		return err
	}
	if srcFileStat.IsDir() {
		return fmt.Errorf("the src file %v of the fetch call should not be dir", srcFilePath)
	}
	if err = os.Rename(srcFilePath, sf.tmpFilePath); err != nil {
		return err
	}

	if srcConfigFileStat, err := os.Stat(srcConfigFilePath); err == nil && !srcConfigFileStat.IsDir() {
		sf.log.Debugf("SyncingFile: found the corresponding src config file %v", srcConfigFilePath)
		if err = os.Rename(srcConfigFilePath, util.GetSyncingFileConfigFilePath(sf.filePath)); err != nil {
			return err
		}
	}

	sf.lock.Lock()
	sf.state = types.StateInProgress
	sf.processedSize = srcFileStat.Size()
	sf.lock.Unlock()

	return nil
}

func (sf *SyncingFile) DownloadFromURL(url string) (written int64, err error) {
	sf.log.Infof("SyncingFile: start to download sync file from URL %v", url)

	needProcessing, err := sf.stateCheckBeforeProcessing()
	if err != nil {
		return 0, err
	}
	if !needProcessing {
		return 0, nil
	}

	defer func() {
		if finalErr := sf.finishProcessing(err); finalErr != nil {
			err = finalErr
		}
	}()

	size, err := sf.handler.GetSizeFromURL(url)
	if err != nil {
		return 0, err
	}
	sf.lock.Lock()
	sf.size = size
	sf.log.WithField("size", size)
	sf.lock.Unlock()

	return sf.handler.DownloadFromURL(sf.ctx, url, sf.tmpFilePath, sf)
}

func (sf *SyncingFile) IdleTimeoutCopyToFile(src io.ReadCloser) (copied int64, err error) {
	sf.log.Infof("SyncingFile: start to copy data to sync file")

	defer func() {
		if err != nil {
			sf.log.Errorf("SyncingFile: failed IdleTimeoutCopyToFile: %v", err)
		}
	}()

	needProcessing, err := sf.stateCheckBeforeProcessing()
	if err != nil {
		return 0, err
	}
	if !needProcessing {
		return 0, nil
	}

	f, err := os.OpenFile(sf.tmpFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if err = f.Truncate(sf.size); err != nil {
		return 0, err
	}

	defer func() {
		if finalErr := sf.finishProcessing(err); finalErr != nil {
			err = finalErr
		}
	}()

	nw, err := IdleTimeoutCopy(sf.ctx, sf.cancel, src, f, sf)
	if err != nil {
		err = errors.Wrapf(err, "failed to copy the data with timeout")
	}
	return nw, err
}

func (sf *SyncingFile) Receive(port int, fileType string) (err error) {
	sf.log.Infof("SyncingFile: start to launch a receiver at port %v", port)

	needProcessing, err := sf.stateCheckBeforeProcessing()
	if err != nil {
		return err
	}
	if !needProcessing {
		return nil
	}

	defer func() {
		if finalErr := sf.finishProcessing(err); finalErr != nil {
			err = finalErr
		}
	}()

	// TODO: After merging the sparse tool repo into this sync service, we don't need to launch a separate server here.
	//  Instead, this SyncingFile is responsible for punching hole, reading/writing data, and computing checksum.
	if serverErr := sparserest.Server(sf.ctx, strconv.Itoa(port), sf.tmpFilePath, sf); serverErr != nil && serverErr != http.ErrServerClosed {
		err = serverErr
		return err
	}

	// For sparse files, holes may not be calculated into s.processedSize.
	// And if the whole file is empty, the state would be starting rather than in-progress.
	// To avoid s.finishProcessing() failure, we need to update s.processedSize in advance.
	sf.lock.Lock()
	if sf.state == types.StateFailed || sf.state == types.StateReady {
		sf.lock.Unlock()
		return nil
	}
	if sf.state == types.StateStarting {
		sf.state = types.StateInProgress
	}
	sf.processedSize = sf.size
	sf.lock.Unlock()

	// The file size will change after conversion.
	if fileType == types.SyncingFileTypeQcow2 {
		sf.log.Infof("SyncingFile: converting the file type from raw to qcow2")
		if err = util.ConvertFromRawToQcow2(sf.tmpFilePath); err != nil {
			return err
		}
	}

	return nil
}

func (sf *SyncingFile) Send(toAddress string, sender Sender) (err error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()

	if sf.sendingReference >= types.SendingLimit {
		return fmt.Errorf("syncing file %v reaches to the simultaneous sending limit %v", sf.filePath, types.SendingLimit)
	}

	sf.validateReadyFileNoLock()
	if sf.state != types.StateReady {
		return fmt.Errorf("invalid state %v for file sending", sf.state)
	}

	sf.sendingReference++
	go func() {
		defer func() {
			sf.lock.Lock()
			sf.sendingReference--
			sf.lock.Unlock()
		}()
		if err := sender(sf.filePath, toAddress); err != nil {
			sf.log.Errorf("SyncingFile: failed to send file: %v", err)
		}
	}()

	return nil
}

func (sf *SyncingFile) finishProcessing(err error) (finalErr error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()

	sf.cancel()

	defer func() {
		sf.handleFailureNoLock(finalErr)
	}()

	if err != nil {
		finalErr = err
		return
	}

	if sf.state != types.StateInProgress {
		sf.log.Warnf("SyncingFile: invalid state %v after processing", sf.state)
		return
	}
	if sf.size > 0 && sf.processedSize != sf.size {
		finalErr = fmt.Errorf("processed data size %v does not match the expected file size %v", sf.processedSize, sf.size)
		return
	}

	stat, statErr := os.Stat(sf.tmpFilePath)
	if statErr != nil {
		finalErr = errors.Wrapf(statErr, "failed to stat tmp file %v after getting the file from source", sf.tmpFilePath)
		return
	}
	if stat.Size() != sf.processedSize {
		sf.log.Debugf("SyncingFile: processed size %v is not equal to the actual file size %v after processing", sf.processedSize, stat.Size())
		sf.processedSize = stat.Size()
	}
	sf.modificationTime = stat.ModTime().UTC().String()

	// Check if there is an existing config file then try to load the checksum
	configFilePath := util.GetSyncingFileConfigFilePath(sf.filePath)
	config, confReadErr := util.ReadSyncingFileConfig(configFilePath)
	if confReadErr != nil {
		if err = os.RemoveAll(configFilePath); err != nil {
			finalErr = errors.Wrapf(err, "failed to clean up the config file %v after read failure", configFilePath)
			return
		}
	}
	if config != nil && config.ModificationTime == sf.modificationTime {
		logrus.Debugf("SyncingFile: directly get the checksum from the valid config during processing wrap-up: %v", config.CurrentChecksum)
		sf.currentChecksum = config.CurrentChecksum
		sf.updateSyncReadyNoLock()
		sf.writeConfigNoLock()

		// Renaming won't change the file modification time.
		if err := os.Rename(sf.tmpFilePath, sf.filePath); err != nil {
			finalErr = errors.Wrapf(err, "failed to rename tmp file %v to file %v", sf.tmpFilePath, sf.filePath)
			return
		}
		sf.log.Info("SyncingFile: succeeded processing file")
		return
	}

	logrus.Debug("SyncingFile: failed to get the checksum from a valid config during processing wrap-up, will directly calculated it then")
	// GetFileChecksum is time consuming, so we make it async as post process function to prevent client timeout,
	go sf.postProcessSyncFile()
	return
}

func (sf *SyncingFile) postProcessSyncFile() {
	var finalErr error
	defer func() {
		sf.lock.Lock()
		sf.handleFailureNoLock(finalErr)
		sf.lock.Unlock()
	}()

	currentChecksum, err := util.GetFileChecksum(sf.tmpFilePath)
	if err != nil {
		finalErr = errors.Wrapf(err, "failed to calculate checksum for tmp file %v", sf.tmpFilePath)
		return
	}

	sf.lock.Lock()
	defer sf.lock.Unlock()
	if modificationTime := util.FileModificationTime(sf.tmpFilePath); modificationTime != sf.modificationTime {
		finalErr = fmt.Errorf("tmp file %v has been modified while calculaing checksum", sf.tmpFilePath)
		return
	}
	sf.currentChecksum = currentChecksum
	if sf.expectedChecksum != "" && sf.expectedChecksum != sf.currentChecksum {
		finalErr = fmt.Errorf("the expected checksum %v doesn't match the file actual checksum %v", sf.expectedChecksum, sf.currentChecksum)
		return
	}

	// If the state is already failed, there is no need to update the state
	if sf.state == types.StateFailed {
		return
	}
	sf.updateSyncReadyNoLock()
	sf.writeConfigNoLock()

	// Renaming won't change the file modification time.
	if err := os.Rename(sf.tmpFilePath, sf.filePath); err != nil {
		finalErr = errors.Wrapf(err, "failed to rename tmp file %v to file %v", sf.tmpFilePath, sf.filePath)
		return
	}

	sf.log.Info("SyncingFile: succeeded processing file")
}

func (sf *SyncingFile) updateSyncReadyNoLock() {
	sf.progress = 100
	sf.size = sf.processedSize
	sf.state = types.StateReady
	sf.log = sf.log.WithFields(logrus.Fields{
		"size":            sf.size,
		"currentChecksum": sf.currentChecksum,
	})
}

func (sf *SyncingFile) handleFailureNoLock(err error) {
	if err == nil {
		return
	}
	if sf.state == types.StateFailed {
		return
	}
	if sf.state == types.StateReady {
		sf.log.Warnf("SyncingFile: file is already state %v, cannot mark it as %v", types.StateReady, types.StateFailed)
		return
	}
	sf.state = types.StateFailed
	if err := os.RemoveAll(sf.tmpFilePath); err != nil {
		sf.log.Warnf("SyncingFile: failed to clean up tmp sync file %v after processing failure, will continue the failure handling: %v", sf.tmpFilePath, err)
	}
	if err := os.RemoveAll(sf.filePath); err != nil {
		sf.log.Warnf("SyncingFile: failed to clean up sync file %v after processing failure, will continue the failure handling: %v", sf.filePath, err)
	}
	sf.message = fmt.Sprintf("failed to process sync file: %v", err)
	sf.log.Errorf("SyncingFile: %s", sf.message)
}

func (sf *SyncingFile) writeConfigNoLock() {
	sf.log.Debugf("SyncingFile: writing config file when the file becomes ready")
	if err := util.WriteSyncingFileConfig(util.GetSyncingFileConfigFilePath(sf.filePath), &util.SyncingFileConfig{
		FilePath:         sf.filePath,
		UUID:             sf.uuid,
		Size:             sf.size,
		ExpectedChecksum: sf.expectedChecksum,
		CurrentChecksum:  sf.currentChecksum,
		ModificationTime: sf.modificationTime,
	}); err != nil {
		sf.log.Warnf("SyncingFile: failed to write config file when the file becomes ready: %v", err)
	}
}
