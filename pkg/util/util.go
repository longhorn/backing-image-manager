package util

import (
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/longhorn/sparse-tools/sparse"
)

func PrintJSON(obj interface{}) error {
	output, err := json.MarshalIndent(obj, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func GetFileChecksum(filePath string) (string, error) {
	f, err := sparse.NewDirectFileIoProcessor(filePath, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// 4MB
	buf := make([]byte, 1<<22)
	h := sha512.New()

	for {
		nr, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				return "", err
			}
			break
		}
		h.Write(buf[:nr])
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// DiskConfigFile should be the same as the schema in longhorn-manager/util
const (
	DiskConfigFile = "longhorn-disk.cfg"
)

type DiskConfig struct {
	DiskUUID string `json:"diskUUID"`
}

func GetDiskConfig(diskPath string) (string, error) {
	filePath := filepath.Join(diskPath, DiskConfigFile)
	output, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("cannot find disk config file %v: %v", filePath, err)
	}

	cfg := &DiskConfig{}
	if err := json.Unmarshal([]byte(output), cfg); err != nil {
		return "", fmt.Errorf("failed to unmarshal %v content %v: %v", filePath, output, err)
	}
	return cfg.DiskUUID, nil
}

// This should be the same as the schema in longhorn-manager/util
const (
	BackingImageConfigFile = "backing.cfg"
)

type BackingImageConfig struct {
	Name             string `json:"name"`
	UUID             string `json:"uuid"`
	Size             int64  `json:"size"`
	ExpectedChecksum string `json:"expectedChecksum"`
	CurrentChecksum  string `json:"currentChecksum"`
}

func WriteBackingImageConfigFile(workDirectory string, cfg *BackingImageConfig) error {
	filePath := filepath.Join(workDirectory, BackingImageConfigFile)
	if _, err := os.Stat(filePath); os.IsExist(err) {
		return fmt.Errorf("backing image cfg on %v exists, cannot override", filePath)
	}

	encoded, err := json.Marshal(cfg)
	if err != nil {
		return errors.Wrapf(err, "BUG: Cannot marshal %+v", cfg)
	}

	defer func() {
		if err != nil {
			if delErr := os.Remove(filePath); delErr != nil && !os.IsNotExist(delErr) {
				err = errors.Wrapf(err, "cleaning up backing image config path %v failed with error: %v", filePath, delErr)
			}
		}
	}()
	return ioutil.WriteFile(filePath, encoded, 0666)
}

func ReadBackingImageConfigFile(workDirectory string) (*BackingImageConfig, error) {
	filePath := filepath.Join(workDirectory, BackingImageConfigFile)
	output, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot find backing image config file %v", filePath)
	}

	cfg := &BackingImageConfig{}
	if err := json.Unmarshal(output, cfg); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal %v content %v", filePath, output)
	}
	return cfg, nil
}
