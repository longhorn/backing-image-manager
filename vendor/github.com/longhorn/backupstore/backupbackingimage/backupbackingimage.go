package backupbackingimage

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/common"
	"github.com/longhorn/backupstore/util"
)

type BackupBackingImage struct {
	sync.Mutex

	Name              string
	Size              int64 `json:",string"`
	BlockCount        int64 `json:",string"`
	Checksum          string
	Labels            map[string]string
	CompressionMethod string
	CreatedTime       string
	CompleteTime      string

	ProcessingBlocks *common.ProcessingBlocks

	Blocks []common.BlockMapping `json:",omitempty"`
}

type BackupConfig struct {
	Name            string
	DestURL         string
	ConcurrentLimit int32
}

type RestoreConfig struct {
	BackupURL       string
	Filename        string
	ConcurrentLimit int32
}

type BackupStatus interface {
	ReadFile(start int64, data []byte) error
	CloseFile() error
	Update(state string, progress int, backupURL string, err string) error
}

type RestoreStatus interface {
	UpdateRestoreProgress(progress int, err error)
}

func CreateBackingImageBackup(config *BackupConfig, backupBackingImage *BackupBackingImage, backupStatus BackupStatus, mappings *common.Mappings) (err error) {
	log := backupstore.GetLog()
	if config == nil || backupBackingImage == nil || backupStatus == nil || mappings == nil {
		return fmt.Errorf("invalid empty config or backupStatus for backup")
	}

	defer func() {
		if err != nil {
			log.WithError(err).Warn("Failed to create backup backing image")
			backupStatus.Update(string(common.ProgressStateError), 0, "", err.Error())
		}
	}()

	bsDriver, err := backupstore.GetBackupStoreDriver(config.DestURL)
	if err != nil {
		return err
	}

	lock, err := backupstore.New(bsDriver, config.Name, backupstore.BACKUP_LOCK)
	if err != nil {
		return err
	}

	defer lock.Unlock()
	if err := lock.Lock(); err != nil {
		return err
	}

	if err := addBackingImageConfigInBackupStore(bsDriver, backupBackingImage); err != nil {
		return err
	}

	backupBackingImage, err = loadBackingImage(bsDriver, backupBackingImage.Name)
	if err != nil {
		return err
	}

	log.Info("Creating backup backing image")

	backupBackingImage.Blocks = []common.BlockMapping{}
	backupBackingImage.ProcessingBlocks = &common.ProcessingBlocks{
		Blocks: map[string][]*common.BlockMapping{},
	}

	if err := lock.Lock(); err != nil {
		return err
	}

	go func() {
		defer backupStatus.CloseFile()
		defer lock.Unlock()

		backupStatus.Update(string(common.ProgressStateInProgress), 0, "", "")

		if progress, backupURL, err := performBackup(bsDriver, config, backupBackingImage, backupStatus, mappings); err != nil {
			log.WithError(err).Errorf("Failed to perform backup for backing image %v", backupBackingImage.Name)
			backupStatus.Update(string(common.ProgressStateInProgress), progress, "", err.Error())
		} else {
			backupStatus.Update(string(common.ProgressStateInProgress), progress, backupURL, "")
		}
	}()

	return nil
}

func performBackup(bsDriver backupstore.BackupStoreDriver, config *BackupConfig,
	backupBackingImage *BackupBackingImage, backupStatus BackupStatus, mappings *common.Mappings) (int, string, error) {
	log := backupstore.GetLog()
	destURL := config.DestURL
	concurrentLimit := config.ConcurrentLimit

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	totalBlockCounts, err := getTotalBackupBlockCounts(mappings)
	if err != nil {
		return 0, "", err
	}
	log.Infof("Creating backup backing image consisting of %v mappings and %v blocks", len(mappings.Mappings), totalBlockCounts)

	progress := &common.Progress{
		TotalBlockCounts: totalBlockCounts,
	}

	mappingChan, errChan := common.PopulateMappings(bsDriver, mappings)

	errorChans := []<-chan error{errChan}
	for i := 0; i < int(concurrentLimit); i++ {
		errorChans = append(errorChans, backupMappings(ctx, bsDriver, config, backupBackingImage, backupStatus, mappings.BlockSize, progress, mappingChan))
	}
	mergedErrChan := common.MergeErrorChannels(ctx, errorChans...)
	err = <-mergedErrChan
	if err != nil {
		return progress.Progress, "", errors.Wrapf(err, "failed to backup backing image %v", backupBackingImage.Name)
	}

	backupBackingImage.Blocks = common.SortBackupBlocks(backupBackingImage.Blocks, backupBackingImage.Size, mappings.BlockSize)
	backupBackingImage.CompleteTime = util.Now()
	backupBackingImage.BlockCount = totalBlockCounts
	if err := saveBackingImageConfig(bsDriver, backupBackingImage); err != nil {
		return progress.Progress, "", err
	}

	return common.ProgressPercentageBackupTotal, EncodeBackupBackingImageURL(config.Name, destURL), nil
}

func backupMappings(ctx context.Context, bsDriver backupstore.BackupStoreDriver,
	config *BackupConfig, backupBackingImage *BackupBackingImage, backupStatus BackupStatus,
	blockSize int64, progress *common.Progress, in <-chan common.Mapping) <-chan error {

	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		for {
			select {
			case <-ctx.Done():
				return
			case mapping, open := <-in:
				if !open {
					return
				}

				if err := backupMapping(bsDriver, config, backupBackingImage, backupStatus, blockSize, mapping, progress); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	return errChan
}

func backupMapping(bsDriver backupstore.BackupStoreDriver,
	config *BackupConfig, backupBackingImage *BackupBackingImage, backupStatus BackupStatus,
	blockSize int64, mapping common.Mapping, progress *common.Progress) error {

	log := backupstore.GetLog()
	block := make([]byte, mapping.Size)

	if err := backupStatus.ReadFile(mapping.Offset, block); err != nil {
		log.WithError(err).Errorf("Failed to read backing image %v block at offset %v size %v", backupBackingImage.Name, mapping.Offset, len(block))
		return err
	}

	if err := backupBlock(bsDriver, config, backupBackingImage, backupStatus, mapping.Offset, block, progress); err != nil {
		logrus.WithError(err).Errorf("Failed to back up backing image %v block at offset %v size %v", backupBackingImage.Name, mapping.Offset, len(block))
		return err
	}

	return nil
}

func backupBlock(bsDriver backupstore.BackupStoreDriver,
	config *BackupConfig, backupBackingImage *BackupBackingImage, backupStatus BackupStatus,
	offset int64, block []byte, progress *common.Progress) error {

	var err error
	newBlock := false

	checksum := util.GetChecksum(block)

	if isBlockBeingProcessed(backupBackingImage, offset, checksum) {
		return nil
	}

	defer func() {
		if err != nil {
			return
		}
		backupBackingImage.Lock()
		defer backupBackingImage.Unlock()
		updateBlocksAndProgress(backupBackingImage, progress, checksum, newBlock)
		backupStatus.Update(string(common.ProgressStateInProgress), progress.Progress, "", "")
	}()

	// skip if block already exists
	blkFile := getBackingImageBlockFilePath(checksum)
	if bsDriver.FileExists(blkFile) {
		return nil
	}

	newBlock = true
	rs, err := util.CompressData(backupBackingImage.CompressionMethod, block)
	if err != nil {
		return err
	}

	return bsDriver.Write(blkFile, rs)
}

func isBlockBeingProcessed(backupBackingImage *BackupBackingImage, offset int64, checksum string) bool {
	processingBlocks := backupBackingImage.ProcessingBlocks

	processingBlocks.Lock()
	defer processingBlocks.Unlock()

	blockInfo := &common.BlockMapping{
		Offset:        offset,
		BlockChecksum: checksum,
	}
	if _, ok := processingBlocks.Blocks[checksum]; ok {
		processingBlocks.Blocks[checksum] = append(processingBlocks.Blocks[checksum], blockInfo)
		return true
	}

	processingBlocks.Blocks[checksum] = []*common.BlockMapping{blockInfo}
	return false
}

func updateBlocksAndProgress(backupBackingImage *BackupBackingImage, progress *common.Progress, checksum string, newBlock bool) {
	processingBlocks := backupBackingImage.ProcessingBlocks

	processingBlocks.Lock()
	defer processingBlocks.Unlock()

	blocks := processingBlocks.Blocks[checksum]
	for _, block := range blocks {
		backupBackingImage.Blocks = append(backupBackingImage.Blocks, *block)
	}

	// Update progress
	func() {
		progress.Lock()
		defer progress.Unlock()

		if newBlock {
			progress.NewBlockCounts++
		}
		progress.ProcessedBlockCounts += int64(len(blocks))
		progress.Progress = common.GetProgress(progress.TotalBlockCounts, progress.ProcessedBlockCounts)
	}()

	delete(processingBlocks.Blocks, checksum)
}

func RestoreBackingImageBackup(config *RestoreConfig, restoreStatus RestoreStatus) error {
	if config == nil || restoreStatus == nil {
		return fmt.Errorf("invalid empty config or restoreStatus for restore")
	}

	backingImageFilePath := config.Filename
	backupURL := config.BackupURL
	concurrentLimit := config.ConcurrentLimit

	bsDriver, err := backupstore.GetBackupStoreDriver(backupURL)
	if err != nil {
		return err
	}

	backingImageName, _, err := DecodeBackupBackingImageURL(backupURL)
	if err != nil {
		return err
	}

	lock, err := backupstore.New(bsDriver, backingImageName, backupstore.RESTORE_LOCK)
	if err != nil {
		return err
	}

	defer lock.Unlock()
	if err := lock.Lock(); err != nil {
		return err
	}

	backupBackingImage, err := loadBackingImage(bsDriver, backingImageName)
	if err != nil {
		return errors.Wrapf(err, "backing image %v doesn't exist in backup store", backingImageName)
	}

	if backupBackingImage.Size == 0 {
		return fmt.Errorf("read invalid backing image size %v", backupBackingImage.Size)
	}

	backingImageFile, err := checkBackingImageFile(backingImageFilePath, backupBackingImage)
	if err != nil {
		return errors.Wrapf(err, "check backing image file failed")
	}

	defer func() {
		if err != nil {
			_ = backingImageFile.Close()
		}
	}()

	if err := lock.Lock(); err != nil {
		return err
	}

	go func() {
		defer backingImageFile.Close()
		defer lock.Unlock()

		progress := &common.Progress{
			TotalBlockCounts: int64(len(backupBackingImage.Blocks)),
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		blockChan, errChan := common.PopulateBlocksForFullRestore(backupBackingImage.Blocks, backupBackingImage.CompressionMethod)
		errorChans := []<-chan error{errChan}
		for i := 0; i < int(concurrentLimit); i++ {
			errorChans = append(errorChans, restoreBlocks(ctx, bsDriver, backingImageFilePath, blockChan, progress, restoreStatus))
		}

		mergedErrChan := common.MergeErrorChannels(ctx, errorChans...)
		err = <-mergedErrChan
		if err != nil {
			restoreStatus.UpdateRestoreProgress(int(progress.ProcessedBlockCounts)*backupstore.DEFAULT_BLOCK_SIZE, err)
			return
		}

		restoreStatus.UpdateRestoreProgress(int(backupBackingImage.Size), nil)
	}()

	return nil
}

func checkBackingImageFile(backingImageFilePath string, backupBackingImage *BackupBackingImage) (*os.File, error) {
	if _, err := os.Stat(backingImageFilePath); err == nil {
		logrus.Warnf("File %s for the restore exists, will remove and re-create it", backingImageFilePath)
		if err := os.RemoveAll(backingImageFilePath); err != nil {
			return nil, errors.Wrapf(err, "failed to clean up the existing file %v before restore", backingImageFilePath)
		}
	}

	backingImageFile, err := os.Create(backingImageFilePath)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = backingImageFile.Close()
		}
	}()

	stat, err := backingImageFile.Stat()
	if err != nil {
		return nil, err
	}

	// This pre-truncate is to ensure the XFS speculatively
	// preallocates post-EOF blocks get reclaimed when volDev is
	// closed.
	// https://github.com/longhorn/longhorn/issues/2503
	// We want to truncate regular files, but not device
	if stat.Mode()&os.ModeType == 0 {
		if err := backingImageFile.Truncate(backupBackingImage.Size); err != nil {
			errors.Wrapf(err, "failed to truncate backing image")
			return nil, err
		}
	}

	return backingImageFile, nil
}

func restoreBlocks(ctx context.Context, bsDriver backupstore.BackupStoreDriver, backingImageFilePath string, in <-chan *common.Block, progress *common.Progress, restoreStatus RestoreStatus) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		backingImageFile, err := os.OpenFile(backingImageFilePath, os.O_RDWR, 0666)
		if err != nil {
			errChan <- err
			return
		}
		defer backingImageFile.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case block, open := <-in:
				if !open {
					return
				}

				if err := restoreBlock(bsDriver, backingImageFile, block, progress, restoreStatus); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	return errChan
}

func restoreBlock(bsDriver backupstore.BackupStoreDriver, backingImageFile *os.File, block *common.Block, progress *common.Progress, restoreStatus RestoreStatus) error {

	defer func() {
		progress.Lock()
		defer progress.Unlock()

		progress.ProcessedBlockCounts++
		progress.Progress = common.GetProgress(progress.TotalBlockCounts, progress.ProcessedBlockCounts)
		restoreStatus.UpdateRestoreProgress(int(progress.ProcessedBlockCounts)*backupstore.DEFAULT_BLOCK_SIZE, nil)
	}()

	return restoreBlockToFile(bsDriver, backingImageFile, block.CompressionMethod,
		common.BlockMapping{
			Offset:        block.Offset,
			BlockChecksum: block.BlockChecksum,
		})
}

func restoreBlockToFile(bsDriver backupstore.BackupStoreDriver, backingImageFile *os.File, decompression string, blk common.BlockMapping) error {
	blkFile := getBackingImageBlockFilePath(blk.BlockChecksum)
	rc, err := bsDriver.Read(blkFile)
	if err != nil {
		return err
	}
	defer rc.Close()
	r, err := util.DecompressAndVerify(decompression, rc, blk.BlockChecksum)
	if err != nil {
		return err
	}

	if _, err := backingImageFile.Seek(blk.Offset, 0); err != nil {
		return err
	}
	_, err = io.Copy(backingImageFile, r)
	return err
}

func RemoveBackingImageBackup(backupURL string) (err error) {
	bsDriver, err := backupstore.GetBackupStoreDriver(backupURL)
	if err != nil {
		return err
	}
	backingImageName, _, err := DecodeBackupBackingImageURL(backupURL)
	if err != nil {
		return err
	}
	log := backupstore.GetLog()
	log = log.WithFields(logrus.Fields{"BackingImage": backingImageName})

	lock, err := backupstore.New(bsDriver, backingImageName, backupstore.DELETION_LOCK)
	if err != nil {
		return err
	}
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	// If we fail to load the backup we still want to proceed with the deletion of the backup file
	backupBackingImage, err := loadBackingImage(bsDriver, backingImageName)
	if err != nil {
		log.WithError(err).Warn("Failed to load to be deleted backup backing image")
		backupBackingImage = &BackupBackingImage{
			Name: backingImageName,
		}
	}

	// we can delete the requested backupBackingImage immediately before GC starts
	if err := removeBackupBackingImage(backupBackingImage, bsDriver); err != nil {
		return err
	}
	log.Info("Removed backup backing image config")

	blockInfos, err := getBlockInfos(bsDriver)
	if err != nil {
		return err
	}

	backupbackingImageNames, err := GetAllBackupBackingImageNames(bsDriver)
	if err != nil {
		log.WithError(err).Warn("Failed to load backup backing image names, skip block deletion")
		return nil
	}

	canDeleteBlocks := checkAndUpdateBlockInfos(bsDriver, blockInfos, backupbackingImageNames)
	if !canDeleteBlocks {
		return nil
	}

	// check if there have been new backups created while we where processing
	prevBackupBackingImageNames := backupbackingImageNames
	backupBackingImageNames, err := GetAllBackupBackingImageNames(bsDriver)
	if err != nil || !util.UnorderedEqual(prevBackupBackingImageNames, backupBackingImageNames) {
		log.Info("Found new backup backing image, skip block deletion")
		return nil
	}

	// only delete the blocks if it is safe to do so
	if err := cleanupBlocks(bsDriver, blockInfos); err != nil {
		return err
	}

	return nil
}

func checkAndUpdateBlockInfos(bsDriver backupstore.BackupStoreDriver, blockInfos map[string]*common.BlockInfo, backupbackingImageNames []string) bool {
	log := backupstore.GetLog()

	for _, name := range backupbackingImageNames {
		backupBackingImage, err := loadBackingImage(bsDriver, name)
		if err != nil {
			log.WithError(err).Warn("Failed to load backup backing image, skip block deletion")
			return false
		}

		if isBackupInProgress(backupBackingImage) {
			log.Info("Found in progress backup backing image, skip block deletion")
			return false
		}

		common.UpdateBlockReferenceCount(blockInfos, backupBackingImage.Blocks, bsDriver)
	}
	return true
}

func getBlockInfos(bsDriver backupstore.BackupStoreDriver) (map[string]*common.BlockInfo, error) {
	blockInfos := make(map[string]*common.BlockInfo)
	blockNames, err := getAllBlockNames(bsDriver)
	if err != nil {
		return nil, err
	}

	for _, name := range blockNames {
		blockInfos[name] = &common.BlockInfo{
			Checksum: name,
			Path:     getBackingImageBlockFilePath(name),
			Refcount: 0,
		}
	}
	return blockInfos, nil
}

func cleanupBlocks(driver backupstore.BackupStoreDriver, blockMap map[string]*common.BlockInfo) error {
	var deletionFailures []string
	deletedBlockCount := int64(0)
	for _, blk := range blockMap {
		if common.IsBlockSafeToDelete(blk) {
			if err := driver.Remove(blk.Path); err != nil {
				deletionFailures = append(deletionFailures, blk.Checksum)
				continue
			}
			deletedBlockCount++
		}
	}

	log := backupstore.GetLog()
	log.Infof("Removed %v blocks", deletedBlockCount)

	if len(deletionFailures) > 0 {
		return fmt.Errorf("failed to delete blocks: %v", deletionFailures)
	}
	return nil
}
