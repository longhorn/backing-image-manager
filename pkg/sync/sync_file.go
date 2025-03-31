package sync

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	butil "github.com/longhorn/backupstore/util"
	lhns "github.com/longhorn/go-common-libs/ns"
	lhtypes "github.com/longhorn/go-common-libs/types"
	sparserest "github.com/longhorn/sparse-tools/sparse/rest"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/backup"
	"github.com/longhorn/backing-image-manager/pkg/crypto"
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

	PeriodicRefreshIntervalInSeconds = 2

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
	virtualSize      int64
	realSize         int64
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

func (sf *SyncingFile) UpdateRestoreProgress(processedSize int, err error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()

	if sf.state == types.StateStarting {
		sf.state = types.StateInProgress
	}
	if sf.state == types.StateReady {
		return
	}
	sf.processedSize = int64(processedSize)
	if sf.size > 0 {
		sf.progress = int((float32(sf.processedSize) / float32(sf.size)) * 100)
	}

	if err != nil {
		sf.message = errors.Wrapf(err, "failed to restore backing image").Error()
	}
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
	sf.updateVirtualSizeNoLock(sf.filePath)
	sf.updateRealSizeNoLock(sf.filePath)
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
		<-ticker.C
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

func (sf *SyncingFile) WaitForStateNonPending() error {
	count := 0
	ticker := time.NewTicker(RetryInterval)
	defer ticker.Stop()
	for {
		<-ticker.C
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
		VirtualSize:      sf.virtualSize,
		RealSize:         sf.realSize,
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

func (sf *SyncingFile) isProcessingRequired() (bool, error) {
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

	needProcessing, err := sf.isProcessingRequired()
	if err != nil {
		return err
	}
	if !needProcessing {
		return nil
	}

	defer func() {
		if finalErr := sf.finishProcessing(err, types.DataEnginev1); finalErr != nil {
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

func (sf *SyncingFile) DownloadFromURL(url, dataEngine string) (written int64, err error) {
	sf.log.Infof("SyncingFile: start to download sync file from URL %v", url)

	needProcessing, err := sf.isProcessingRequired()
	if err != nil {
		return 0, err
	}
	if !needProcessing {
		return 0, nil
	}

	defer func() {
		if finalErr := sf.finishProcessing(err, dataEngine); finalErr != nil {
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

func (sf *SyncingFile) RestoreFromBackupURL(backupURL string, credential map[string]string, concurrentLimit int, dataEngine string) (err error) {
	sf.log.Infof("SyncingFile: start to restore sync file from backup URL %v", backupURL)

	needProcessing, err := sf.isProcessingRequired()
	if err != nil {
		return err
	}
	if !needProcessing {
		return nil
	}

	defer func() {
		if finalErr := sf.finishProcessing(err, dataEngine); finalErr != nil {
			err = finalErr
		}
	}()

	backupType, err := util.CheckBackupType(backupURL)
	if err != nil {
		return errors.Wrapf(err, "failed to check the type for backup %v", backupURL)
	}

	if err := butil.SetupCredential(backupType, credential); err != nil {
		return err
	}

	backupURL = butil.UnescapeURL(backupURL)

	info, err := backup.GetBackupInfo(backupURL)
	if err != nil {
		return err
	}

	sf.lock.Lock()
	sf.size = info.Size
	sf.lock.Unlock()

	// async call to start restoration
	if err := backup.DoBackupRestore(backupURL, sf.tmpFilePath, concurrentLimit, sf); err != nil {
		return err
	}

	// wait until restoration is complete or failed
	err = sf.waitForRestoreComplete()
	return err
}

func (sf *SyncingFile) waitForRestoreComplete() (err error) {
	var (
		restoreProgress int
		restoreError    string
	)
	periodicChecker := time.NewTicker(PeriodicRefreshIntervalInSeconds * time.Second)

	for range periodicChecker.C {
		sf.lock.Lock()
		restoreProgress = sf.progress
		restoreError = sf.message
		sf.lock.Unlock()
		if restoreProgress == 100 {
			periodicChecker.Stop()
			return nil
		}
		if restoreError != "" {
			periodicChecker.Stop()
			return fmt.Errorf("%v", restoreError)
		}
	}
	return nil
}

// CloneToFileWithEncryption clone the backing file on the same node to another backing file with the given encryption operation.
// when doing encryption, it creates a loop device from the target backing file, setup the encrypted device from the loop device and then dump the data from the source file to the target encrypted device.
// When doing decryption, it creates a loop device from the source backing file, setup the encrypted device from the loop device and then dump the data from the source encrypted device to the target file.
// When doing ignore clone, it directly dumps the data from the source backing file to the target backing file.
func (sf *SyncingFile) CloneToFileWithEncryption(sourceBackingImage, sourceBackingImageUUID string, encryption types.EncryptionType, credential map[string]string, dataEngine string) (copied int64, err error) {
	sf.log.Infof("SyncingFile: start to clone the file")

	defer func() {
		if err != nil {
			sf.log.Errorf("SyncingFile: failed CloneToFileWithEncryption: %v", err)
		}
	}()

	needProcessing, err := sf.isProcessingRequired()
	if err != nil {
		return 0, err
	}
	if !needProcessing {
		return 0, nil
	}
	defer func() {
		if finalErr := sf.finishProcessing(err, dataEngine); finalErr != nil {
			err = finalErr
		}
	}()

	sourceFile, tmpRawFile, writeZero, err := sf.prepareCloneSourceFile(sourceBackingImage, sourceBackingImageUUID, encryption)
	defer func() {
		if tmpRawFile != "" {
			if errRemove := os.RemoveAll(tmpRawFile); errRemove != nil {
				sf.log.WithError(errRemove).Errorf("Failed to remove the tmp raw file %v", tmpRawFile)
			}
		}
	}()
	if err != nil {
		return 0, errors.Wrapf(err, "failed to prepare source file")
	}

	err = sf.prepareCloneTargetFile(sourceFile, encryption)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to prepare target file")
	}

	sourceFileReader, sourceLoopDevicePath, err := sf.openCloneSourceFile(sourceFile, encryption, credential)
	defer func() {
		if sourceFileReader != nil {
			if errClose := sourceFileReader.Close(); errClose != nil {
				sf.log.WithError(errClose).Error("Failed to close the source file reader")
			}
		}
		if sourceLoopDevicePath == "" {
			sf.closeCryptoDevice(sourceLoopDevicePath)
		}
	}()
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open clone source file")
	}

	targetFileWriter, targetLoopDevicePath, err := sf.openCloneTargetFile(encryption, credential)
	defer func() {
		if targetFileWriter != nil {
			if errClose := targetFileWriter.Close(); errClose != nil {
				sf.log.WithError(errClose).Error("Failed to close the target file writer")
			}
		}
		if targetLoopDevicePath == "" {
			sf.closeCryptoDevice(targetLoopDevicePath)
		}
	}()
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open clone source file")
	}

	nw, err := IdleTimeoutCopy(sf.ctx, sf.cancel, sourceFileReader, targetFileWriter, sf, writeZero)
	if err != nil {
		err = errors.Wrapf(err, "failed to copy the data with timeout")
	}
	return nw, err
}

func (sf *SyncingFile) openCloneSourceFile(sourceFile string, encryption types.EncryptionType, credential map[string]string) (*os.File, string, error) {
	loopDevicePath := ""
	if encryption == types.EncryptionTypeDecrypt {
		loopDevicePath, err := sf.setupCryptoDevice(sourceFile, false, credential)
		if err != nil {
			return nil, loopDevicePath, errors.Wrapf(err, "failed to setup the crypto device with the file %v during cloning", sourceFile)
		}
		sourceFile = types.BackingImageMapper(sf.uuid)
	}

	if _, err := os.Stat(sourceFile); err != nil {
		return nil, loopDevicePath, errors.Wrapf(err, "%v not found during cloning", sourceFile)
	}

	sourceFileReader, err := os.Open(sourceFile)
	if err != nil {
		return nil, loopDevicePath, errors.Wrapf(err, "Error opening file %v", sourceFile)
	}

	return sourceFileReader, loopDevicePath, nil
}

func (sf *SyncingFile) openCloneTargetFile(encryption types.EncryptionType, credential map[string]string) (*os.File, string, error) {
	loopDevicePath := ""
	targetFile := sf.tmpFilePath

	if encryption == types.EncryptionTypeEncrypt {
		loopDevicePath, err := sf.setupCryptoDevice(targetFile, true, credential)
		if err != nil {
			return nil, loopDevicePath, errors.Wrapf(err, "failed to setup the crypto device with the file %v during cloning", targetFile)
		}
		targetFile = types.BackingImageMapper(sf.uuid)
	}

	if _, err := os.Stat(targetFile); err != nil {
		return nil, loopDevicePath, errors.Wrapf(err, "%v not found during cloning", targetFile)
	}

	targetFileWriter, err := os.OpenFile(targetFile, os.O_RDWR, 0666)
	if err != nil {
		return nil, loopDevicePath, errors.Wrapf(err, "Error opening file %v", targetFile)
	}
	return targetFileWriter, loopDevicePath, nil
}

func (sf *SyncingFile) prepareCloneSourceFile(sourceBackingImage, sourceBackingImageUUID string, encryption types.EncryptionType) (string, string, bool, error) {
	writeZero := false
	sourceFile := types.GetBackingImageFilePath(types.DiskPathInContainer, sourceBackingImage, sourceBackingImageUUID)
	tmpRawFile := ""

	if _, err := os.Stat(sourceFile); err != nil {
		return "", tmpRawFile, writeZero, errors.Wrapf(err, "source file %v not found ", sourceFile)
	}

	if encryption == types.EncryptionTypeEncrypt {
		// we should write zero when doing encryption so the zero can be encrypted as well
		writeZero = true

		// If the source file is qcow2 when encrypting we need to convert and use its raw image
		imgInfo, err := util.GetQemuImgInfo(sourceFile)
		if err != nil {
			return "", tmpRawFile, writeZero, errors.Wrapf(err, "failed to get source backing file %v qemu info", sourceFile)
		}
		if imgInfo.Format == "qcow2" {
			tmpRawFile := fmt.Sprintf("%v-raw.tmp", sourceFile)

			if err := util.ConvertFromQcow2ToRaw(sourceFile, tmpRawFile); err != nil {
				return "", tmpRawFile, writeZero, errors.Wrapf(err, "failed to create raw image from qcow2 image %v", sourceFile)
			}
			// use the raw image as source when doing encryption
			sourceFile = tmpRawFile
		}
	}

	return sourceFile, tmpRawFile, writeZero, nil
}

func (sf *SyncingFile) prepareCloneTargetFile(sourceFile string, encryption types.EncryptionType) error {
	info, err := os.Stat(sourceFile)
	if err != nil {
		return err
	}
	sourceFileSize := info.Size()
	if err := sf.setFileSizeForEncryption(sourceFileSize, encryption); err != nil {
		return errors.Wrap(err, "failed to set size for the target file")
	}
	f, err := os.OpenFile(sf.tmpFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	if err = f.Truncate(sf.size); err != nil {
		return err
	}
	if errClose := f.Close(); errClose != nil {
		logrus.WithError(errClose).Error("Failed to close the file")
	}
	return nil
}

func (sf *SyncingFile) IdleTimeoutCopyToFile(src io.ReadCloser, dataEngine string) (copied int64, err error) {
	sf.log.Infof("SyncingFile: start to copy data to sync file")

	defer func() {
		if err != nil {
			sf.log.Errorf("SyncingFile: failed IdleTimeoutCopyToFile: %v", err)
		}
	}()

	needProcessing, err := sf.isProcessingRequired()
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
	defer func() {
		if errClose := f.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close the file")
		}
	}()
	if err = f.Truncate(sf.size); err != nil {
		return 0, err
	}

	defer func() {
		if finalErr := sf.finishProcessing(err, dataEngine); finalErr != nil {
			err = finalErr
		}
	}()

	nw, err := IdleTimeoutCopy(sf.ctx, sf.cancel, src, f, sf, false)
	if err != nil {
		err = errors.Wrapf(err, "failed to copy the data with timeout")
	}
	return nw, err
}

func (sf *SyncingFile) Receive(port int, fileType, dataEngine string) (err error) {
	sf.log.Infof("SyncingFile: start to launch a receiver at port %v", port)

	needProcessing, err := sf.isProcessingRequired()
	if err != nil {
		return err
	}
	if !needProcessing {
		return nil
	}

	defer func() {
		if finalErr := sf.finishProcessing(err, dataEngine); finalErr != nil {
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

func (sf *SyncingFile) finishProcessing(err error, dataEngine string) (finalErr error) {
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

	// If the file is qcow2, we need to convert it to raw for dumping the data to the spdk lvol
	// This will only happen when preparing the first backing image in data source.
	if dataEngine == types.DataEnginev2 {
		imgInfo, qemuErr := util.GetQemuImgInfo(sf.tmpFilePath)
		if qemuErr != nil {
			finalErr = errors.Wrapf(qemuErr, "failed to detect if file %v is qcow2", sf.tmpFilePath)
			return
		}
		tmpRawFile := fmt.Sprintf("%v-raw.tmp", sf.tmpFilePath)
		if imgInfo.Format == "qcow2" {
			if convertErr := util.ConvertFromQcow2ToRaw(sf.tmpFilePath, tmpRawFile); convertErr != nil {
				finalErr = errors.Wrapf(convertErr, "failed to create raw image from qcow2 image %v", sf.tmpFilePath)
				return
			}
			if removeErr := os.RemoveAll(sf.tmpFilePath); removeErr != nil {
				sf.log.Warnf("SyncingFile: failed to remove the qcow2 file %v after converting to raw file", sf.tmpFilePath)
			}
			if renameErr := os.Rename(tmpRawFile, sf.tmpFilePath); renameErr != nil {
				finalErr = errors.Wrapf(renameErr, "failed to rename tmp raw file %v to file %v", tmpRawFile, sf.tmpFilePath)
				return
			}
		}

		stat, statErr := os.Stat(sf.tmpFilePath)
		if statErr != nil {
			finalErr = errors.Wrapf(statErr, "failed to stat tmp file %v after converting from qcow2 to raw file", sf.tmpFilePath)
			return
		}
		sf.size = stat.Size()
		sf.processedSize = stat.Size()
		sf.modificationTime = stat.ModTime().UTC().String()
	}

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
		sf.updateVirtualSizeNoLock(sf.tmpFilePath)
		sf.updateRealSizeNoLock(sf.tmpFilePath)
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
	sf.updateVirtualSizeNoLock(sf.tmpFilePath)
	sf.updateRealSizeNoLock(sf.tmpFilePath)
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

func (sf *SyncingFile) updateVirtualSizeNoLock(filePath string) {
	// This only works if filePath is valid - sometimes we need to call it
	// with sf.tmpFilePath, sometimes with sf.filePath :-/
	imgInfo, err := util.GetQemuImgInfo(filePath)
	if err != nil {
		sf.log.WithError(err).Warnf("SyncingFile: failed to get backing image virtual size")
	}
	// This will be zero when there is an error, which allows components
	// further up the stack to know that the virtual size somehow isn't
	// available yet.
	sf.virtualSize = imgInfo.VirtualSize
}

func (sf *SyncingFile) updateRealSizeNoLock(filePath string) {
	realSize, err := util.GetFileRealSize(filePath)
	if err != nil {
		sf.log.WithError(err).Warnf("SyncingFile: failed to get backing image virtual size")
	}

	// This will be zero when there is an error, which allows components
	// further up the stack to know that the real size somehow isn't
	// available yet.
	sf.realSize = realSize
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
		VirtualSize:      sf.virtualSize,
		RealSize:         sf.realSize,
		ExpectedChecksum: sf.expectedChecksum,
		CurrentChecksum:  sf.currentChecksum,
		ModificationTime: sf.modificationTime,
	}); err != nil {
		sf.log.Warnf("SyncingFile: failed to write config file when the file becomes ready: %v", err)
	}
}

func (sf *SyncingFile) setFileSizeForEncryption(sourceSize int64, encryption types.EncryptionType) error {
	sf.lock.Lock()
	if encryption == types.EncryptionTypeIgnore { // nolint: staticcheck
		sf.size = sourceSize
	} else if encryption == types.EncryptionTypeEncrypt {
		// LUKS 16MB data size is introduced here: https://gitlab.com/cryptsetup/cryptsetup/-/blob/master/docs/v2.1.0-ReleaseNotes#L27
		sf.size = sourceSize + types.EncryptionMetaSize
		// The meta data size won't be counted when copying the data
		// need to init the processedSize with the meta data size
		sf.processedSize = types.EncryptionMetaSize
	} else if encryption == types.EncryptionTypeDecrypt {
		sf.size = sourceSize - types.EncryptionMetaSize
	}
	sf.lock.Unlock()

	if sf.size < 0 {
		return fmt.Errorf("file size is smaller than 0 while encryption or decryption")
	}
	return nil
}

func (sf *SyncingFile) setupCryptoDevice(file string, needFormat bool, credential map[string]string) (string, error) {
	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceNet}
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.ProcDirectory, namespaces)
	if err != nil {
		return "", err
	}

	output, err := nsexec.Execute(nil, "losetup", []string{"-f"}, types.CommandExecutionTimeout)
	if err != nil {
		return "", err
	}
	loopDevicePath := strings.TrimSpace(output)
	if loopDevicePath == "" {
		return "", fmt.Errorf("failed to get valid loop device path")
	}

	if _, err := nsexec.Execute(nil, "losetup", []string{loopDevicePath, file}, types.CommandExecutionTimeout); err != nil {
		return loopDevicePath, err
	}

	keyProvider := credential[lhtypes.CryptoKeyProvider]
	passphrase := credential[lhtypes.CryptoKeyValue]
	if keyProvider != "" && keyProvider != "secret" {
		return loopDevicePath, fmt.Errorf("unsupported key provider %v for encryption", keyProvider)
	}
	if len(passphrase) == 0 {
		return loopDevicePath, fmt.Errorf("missing passphrase for encryption")
	}

	// If we are working on encryption, the target device has not been formatted with the cryptsetup and it needs to be formatted first.
	// If we are working on decryption, the source is a encrypted file and we don't need to format the source device again or the metadata will be replaced.
	if needFormat {
		cryptoParams := crypto.NewEncryptParams(keyProvider, credential[lhtypes.CryptoKeyCipher], credential[lhtypes.CryptoKeyHash], credential[lhtypes.CryptoKeySize], credential[lhtypes.CryptoPBKDF])
		if err := crypto.EncryptBackingImage(loopDevicePath, passphrase, cryptoParams); err != nil {
			return loopDevicePath, err
		}
	}

	if err := crypto.OpenBackingImage(loopDevicePath, passphrase, sf.uuid); err != nil {
		return loopDevicePath, err
	}

	return loopDevicePath, nil
}

func (sf *SyncingFile) closeCryptoDevice(loopDevicePath string) {
	if err := crypto.CloseBackingImage(sf.uuid); err != nil {
		sf.log.WithError(err).Warnf("failed to close the crypto device of backing file")
	}

	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceNet}
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.ProcDirectory, namespaces)
	if err != nil {
		sf.log.WithError(err).Warnf("failed to setup nsexec to detach the loop device after cloning")
	}

	output, err := nsexec.Execute(nil, "losetup", []string{"-d", loopDevicePath}, types.CommandExecutionTimeout)
	if err != nil {
		sf.log.WithError(err).Warnf("failed to detach the loop device after cloning, output: %v", output)
	}
}
