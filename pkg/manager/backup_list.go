package manager

import (
	"fmt"
	"sync"

	"github.com/longhorn/backing-image-manager/pkg/backingimage"
)

const (
	MaxBackupSize = 5
)

type BackupList struct {
	sync.RWMutex
	infos []*BackupInfo
}

type BackupInfo struct {
	backupID     string
	backupStatus *backingimage.BackupStatus
}

// The slice Backup.backupList is implemented similar to a FIFO queue.

// BackupGet takes backupID input and will return the backup object corresponding to that backupID or error if not found
func (b *BackupList) BackupGet(backupID string) (*backingimage.BackupStatus, error) {
	if backupID == "" {
		return nil, fmt.Errorf("empty backupID")
	}

	if err := b.refresh(); err != nil {
		return nil, err
	}

	b.RLock()
	defer b.RUnlock()

	for _, info := range b.infos {
		if info.backupID == backupID {
			return info.backupStatus, nil
		}
	}
	return nil, fmt.Errorf("backup backing image not found %v", backupID)
}

// BackupAdd creates a new backupList object and appends to the end of the list maintained by backup object
func (b *BackupList) BackupAdd(backupID string, BackupStatus *backingimage.BackupStatus) error {
	if backupID == "" {
		return fmt.Errorf("empty backupID")
	}

	b.Lock()
	b.infos = append(b.infos, &BackupInfo{
		backupID:     backupID,
		backupStatus: BackupStatus,
	})
	b.Unlock()

	err := b.refresh()
	return err
}

// remove deletes the object present at slice[index] and returns the remaining elements of slice yet maintaining
// the original order of elements in the slice
func (*BackupList) remove(b []*BackupInfo, index int) ([]*BackupInfo, error) {
	if b == nil {
		return nil, fmt.Errorf("empty list")
	}
	if index >= len(b) || index < 0 {
		return nil, fmt.Errorf("BUG: attempting to delete an out of range index entry from backupList")
	}
	return append(b[:index], b[index+1:]...), nil
}

// Refresh deletes all the old completed backups from the front. Old backups are the completed backups
// that are created before MaxBackupSize completed backups
func (b *BackupList) refresh() error {
	b.Lock()
	defer b.Unlock()

	var index, completed int

	for index = len(b.infos) - 1; index >= 0; index-- {
		if b.infos[index].backupStatus.Progress == 100 {
			if completed == MaxBackupSize {
				break
			}
			completed++
		}
	}
	if completed == MaxBackupSize {
		// Remove all the older completed backups in the range backupList[0:index]
		for ; index >= 0; index-- {
			if b.infos[index].backupStatus.Progress == 100 {
				updatedList, err := b.remove(b.infos, index)
				if err != nil {
					return err
				}
				b.infos = updatedList
				// As this backupList[index] is removed, will have to decrement the index by one
				index--
			}
		}
	}
	return nil
}
