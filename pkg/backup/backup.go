package backup

import (
	"fmt"
	"os"

	"github.com/longhorn/backing-image-manager/pkg/backingimage"
	"github.com/longhorn/backing-image-manager/pkg/util"
	"github.com/longhorn/backupstore/backupbackingimage"
	engineutil "github.com/longhorn/longhorn-engine/pkg/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	// Involve backupstore drivers for registration
	_ "github.com/longhorn/backupstore/azblob"
	_ "github.com/longhorn/backupstore/cifs"
	_ "github.com/longhorn/backupstore/nfs"
	_ "github.com/longhorn/backupstore/s3"
	_ "github.com/longhorn/backupstore/vfs"
)

type CreateBackupParameters struct {
	Name              string
	Path              string
	Checksum          string
	DestURL           string
	CompressionMethod string
	ConcurrentLimit   int32
	Labels            []string
}

func DoBackupInit(params *CreateBackupParameters) (*backupbackingimage.BackupBackingImage, *backingimage.BackupStatus, *backupbackingimage.BackupConfig, error) {
	log := logrus.WithFields(logrus.Fields{"pkg": "backup"})
	log.Infof("Initializing backup for backingimage %v ", params.Name)

	var err error

	if params.DestURL == "" {
		return nil, nil, nil, fmt.Errorf("missing input parameter")
	}

	if !util.ValidBackingImageName(params.Name) {
		return nil, nil, nil, fmt.Errorf("invalid backing image name %v for it is longer than 64", params.Name)
	}

	var labelMap map[string]string
	if params.Labels != nil {
		labelMap, err = engineutil.ParseLabels(params.Labels)
		if err != nil {
			return nil, nil, nil, errors.Wrapf(err, "failed to parse backup labels for backup backing image %v", params.Name)
		}
	}

	backingImage, err := openBackingImage(params.Path)
	if err != nil {
		return nil, nil, nil, err
	}

	backupStatus := backingimage.NewBackupStatus(params.Name, backingImage)

	backupBackingImage := &backupbackingimage.BackupBackingImage{
		Name:              params.Name,
		Size:              backingImage.Size,
		Checksum:          params.Checksum,
		Labels:            labelMap,
		CompressionMethod: params.CompressionMethod,
		CreatedTime:       engineutil.Now(),
	}

	backupConfig := &backupbackingimage.BackupConfig{
		Name:            params.Name,
		ConcurrentLimit: params.ConcurrentLimit,
		DestURL:         params.DestURL,
	}
	return backupBackingImage, backupStatus, backupConfig, nil
}

func DoBackupCreate(
	backupBackingImage *backupbackingimage.BackupBackingImage,
	backupStatus *backingimage.BackupStatus,
	backupConfig *backupbackingimage.BackupConfig,
) error {
	log := logrus.WithFields(logrus.Fields{"pkg": "backup"})
	log.Infof("Creating backup backing image %v", backupStatus.Name)

	mappings, err := backingimage.CreateBackupBackingImageMappings(backupStatus.BackingImage)
	if err != nil {
		return err
	}

	err = backupbackingimage.CreateBackingImageBackup(backupConfig, backupBackingImage, backupStatus, mappings)
	return err
}

func DoBackupRestore(backupURL string, toFile string, concurrentLimit int, restoreStatus backupbackingimage.RestoreStatus) error {
	log := logrus.WithFields(logrus.Fields{"pkg": "backup"})
	log.Infof("Restoring from %v into backing image %v", backupURL, toFile)
	backupURL = engineutil.UnescapeURL(backupURL)

	return backupbackingimage.RestoreBackingImageBackup(&backupbackingimage.RestoreConfig{
		BackupURL:       backupURL,
		Filename:        toFile,
		ConcurrentLimit: int32(concurrentLimit),
	}, restoreStatus)
}

func GetBackupInfo(backupURL string) (*backupbackingimage.BackupInfo, error) {
	return backupbackingimage.InspectBackupBackingImage(backupURL)
}

func openBackingImage(path string) (*backingimage.BackingImage, error) {
	if path == "" {
		return nil, nil
	}

	if _, err := os.Stat(path); err != nil {
		return nil, err
	}

	return backingimage.OpenBackingImage(path)
}
