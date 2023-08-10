package backingimage

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/common"
	enginetypes "github.com/longhorn/longhorn-engine/pkg/types"
	engineutil "github.com/longhorn/longhorn-engine/pkg/util"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
	"github.com/longhorn/sparse-tools/sparse"
	"github.com/rancher/go-fibmap"
	"github.com/sirupsen/logrus"
)

const (
	MaxExtentsBuffer = 1024
)

type BackingImage struct {
	Size       int64
	SectorSize int64
	Path       string
	Disk       enginetypes.DiffDisk
	Format     string

	rmLock   sync.Mutex
	Location []byte
}

// Preload populates r.volume.location with correct values
func (bi *BackingImage) Preload() error {
	if bi.Format == "qcow2" {
		bi.initializeSectorLocation(byte(1))
		return nil
	}
	bi.initializeSectorLocation(byte(0))
	bi.preload()
	return nil
}

func (bi *BackingImage) initializeSectorLocation(value byte) {
	for i := 0; i < len(bi.Location); i++ {
		bi.Location[i] = value
	}
}

func (bi *BackingImage) preload() error {
	return loadBackingImageLocation(bi, bi.Disk)
}

func loadBackingImageLocation(backingimage *BackingImage, disk enginetypes.DiffDisk) error {
	fd := disk.Fd()
	start := uint64(0)
	end := uint64(len(backingimage.Location)) * uint64(backingimage.SectorSize)
	for {
		extents, errno := fibmap.Fiemap(fd, start, end-start, MaxExtentsBuffer)
		if errno != 0 {
			return errno
		}

		if len(extents) == 0 {
			return nil
		}

		for _, extent := range extents {
			for i := int64(0); i < int64(extent.Length); i += backingimage.SectorSize {
				backingimage.Location[(int64(extent.Logical)+i)/backingimage.SectorSize] = byte(1)
			}
			if extent.Flags&fibmap.FIEMAP_EXTENT_LAST != 0 {
				return nil
			}
		}

		start = extents[len(extents)-1].Logical + extents[len(extents)-1].Length
	}
}

func (bi *BackingImage) Close() {
	bi.Disk.Close()
}

func (bi *BackingImage) ReadAt(buf []byte, offset int64) (int, error) {
	startOffset := offset % bi.SectorSize
	startCut := bi.SectorSize - startOffset
	endOffset := (int64(len(buf)) + offset) % bi.SectorSize

	if len(buf) == 0 {
		return 0, nil
	}
	if startOffset == 0 && endOffset == 0 {
		return bi.fullReadAt(buf, offset)
	}

	readBuf := make([]byte, bi.SectorSize)
	if _, err := bi.fullReadAt(readBuf, offset-startOffset); err != nil {
		return 0, err
	}

	copy(buf, readBuf[startOffset:])

	if startCut >= int64(len(buf)) {
		return len(buf), nil
	}

	if _, err := bi.fullReadAt(buf[startCut:int64(len(buf))-endOffset], offset+startCut); err != nil {
		return 0, err
	}

	if endOffset > 0 {
		if _, err := bi.fullReadAt(readBuf, offset+int64(len(buf))-endOffset); err != nil {
			return 0, err
		}

		copy(buf[int64(len(buf))-endOffset:], readBuf[:endOffset])
	}

	return len(buf), nil
}

func (bi *BackingImage) fullReadAt(buf []byte, offset int64) (int, error) {
	if int64(len(buf))%bi.SectorSize != 0 || offset%bi.SectorSize != 0 {
		return 0, fmt.Errorf("read not a multiple of %d", bi.SectorSize)
	}

	if len(buf) == 0 {
		return 0, nil
	}

	count := 0
	sectors := int64(len(buf)) / bi.SectorSize
	readSectors := int64(1)

	for i := int64(1); i < sectors; i++ {
		readSectors++
	}

	if readSectors > 0 {
		c, err := bi.read(buf, offset, sectors-readSectors, readSectors)
		count += c
		if err != nil {
			return count, err
		}
	}

	return count, nil
}

func (bi *BackingImage) read(buf []byte, startOffset int64, startSector int64, sectors int64) (int, error) {
	bufStart := startSector * bi.SectorSize
	bufLength := sectors * bi.SectorSize
	offset := startOffset + bufStart
	size, err := bi.Disk.Size()
	if err != nil {
		return 0, err
	}

	// Reading the out-of-bound part is not allowed
	if bufLength > bi.Size-offset {
		logrus.Warn("Trying to read the out-of-bound part")
		return 0, io.ErrUnexpectedEOF
	}

	// May read the expanded part
	if offset >= size {
		return 0, nil
	}
	var newBuf []byte
	if bufLength > size-offset {
		newBuf = buf[bufStart : bufStart+size-offset]
	} else {
		newBuf = buf[bufStart : bufStart+bufLength]
	}
	return bi.Disk.ReadAt(newBuf, offset)
}

func OpenBackingImage(path string) (*BackingImage, error) {
	file, err := engineutil.ResolveBackingFilepath(path)
	if err != nil {
		return nil, err
	}

	format, err := util.DetectFileFormat(file)
	if err != nil {
		return nil, err
	}

	var f enginetypes.DiffDisk
	switch format {
	case "qcow2":
		// This is only used when doing backup.
		// We open qcow2 like raw file and simply backup all the blocks of the qcow2
		// because qcow2 is not like sparse file which can find hole and data with fiemap
		if f, err = sparse.NewDirectFileIoProcessor(file, os.O_RDONLY, 04444, false); err != nil {
			return nil, err
		}
	case "raw":
		if f, err = sparse.NewDirectFileIoProcessor(file, os.O_RDONLY, 04444, false); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("format %v of the backing file %v is not supported", format, file)
	}

	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	if size%diskutil.BackingImageSectorSize != 0 {
		return nil, fmt.Errorf("the backing file size %v should be a multiple of %v bytes since Longhorn uses directIO by default", size, diskutil.BackingImageSectorSize)
	}

	locationSize := size / diskutil.BackingImageSectorSize
	if size%diskutil.BackingImageSectorSize != 0 {
		locationSize++
	}
	location := make([]byte, locationSize)

	return &BackingImage{
		Size:       size,
		SectorSize: diskutil.BackingImageSectorSize,
		Path:       file,
		Disk:       f,
		Format:     format,

		Location: location,
	}, nil
}

func CreateBackupBackingImageMappings(backingImage *BackingImage) (*common.Mappings, error) {
	if err := backingImage.Preload(); err != nil {
		return nil, err
	}

	mappings := &common.Mappings{
		BlockSize: backupstore.DEFAULT_BLOCK_SIZE,
	}
	mapping := common.Mapping{
		Offset: -1,
	}

	for i, val := range backingImage.Location {
		if val != byte(0) {
			offset := int64(i) * types.DefaultSectorSize
			offset -= (offset % backupstore.DEFAULT_BLOCK_SIZE)
			blockSize := int64(backupstore.DEFAULT_BLOCK_SIZE)
			if mapping.Offset != offset {
				if offset+backupstore.DEFAULT_BLOCK_SIZE > backingImage.Size {
					blockSize = backingImage.Size - offset
				}
				mapping = common.Mapping{
					Offset: offset,
					Size:   blockSize,
				}
				mappings.Mappings = append(mappings.Mappings, mapping)
			}
		}
	}

	return mappings, nil
}

type BackupStatus struct {
	lock         sync.Mutex
	Name         string
	BackingImage *BackingImage
	Error        string
	Progress     int
	BackupURL    string
	State        common.ProgressState
	IsOpened     bool
}

func NewBackupStatus(name string, backingImage *BackingImage) *BackupStatus {
	return &BackupStatus{
		Name:         name,
		BackingImage: backingImage,
		State:        common.ProgressStateInProgress,
	}
}

func (b *BackupStatus) CloseFile() error {
	b.BackingImage.Close()
	return nil
}

func (b *BackupStatus) ReadFile(start int64, data []byte) error {
	_, err := b.BackingImage.ReadAt(data, start)
	return err
}

func (b *BackupStatus) Update(state string, progress int, backupURL string, err string) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.State = common.ProgressState(state)
	b.Progress = progress
	b.BackupURL = backupURL
	b.Error = err

	if b.Progress == 100 {
		b.State = common.ProgressStateComplete
	} else if b.Error != "" {
		b.State = common.ProgressStateError
	}
	return nil
}
