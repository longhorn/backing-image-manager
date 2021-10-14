package util

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
)

const (
	QemuImgBinary = "qemu-img"
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
		_, err = h.Write(buf[:nr])
		if err != nil {
			return "", err
		}
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

func Execute(envs []string, binary string, args ...string) (string, error) {
	return ExecuteWithTimeout(time.Minute, envs, binary, args...)
}

func ExecuteWithTimeout(timeout time.Duration, envs []string, binary string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var err error
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), envs...)
	done := make(chan struct{})

	var output, stderr bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &stderr

	go func() {
		err = cmd.Run()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				logrus.Warnf("problem killing process pid=%v: %s", cmd.Process.Pid, err)
			}
		}
		return "", fmt.Errorf("timeout executing: %v %v, output %s, stderr, %s, error %v",
			binary, args, output.String(), stderr.String(), err)
	}

	if err != nil {
		return "", fmt.Errorf("failed to execute: %v %v, output %s, stderr, %s, error %v",
			binary, args, output.String(), stderr.String(), err)
	}
	return output.String(), nil
}

func DetectFileFormat(filePath string) (string, error) {

	/* Example command outputs
	   $ qemu-img info parrot.raw
	   image: parrot.raw
	   file format: raw
	   virtual size: 32M (33554432 bytes)
	   disk size: 2.2M

	   $ qemu-img info parrot.qcow2
	   image: parrot.qcow2
	   file format: qcow2
	   virtual size: 32M (33554432 bytes)
	   disk size: 2.3M
	   cluster_size: 65536
	   Format specific information:
	       compat: 1.1
	       lazy refcounts: false
	       refcount bits: 16
	       corrupt: false
	*/

	output, err := Execute([]string{}, QemuImgBinary, "info", filePath)
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "file format: ") {
			return strings.TrimPrefix(line, "file format: "), nil
		}
	}

	return "", fmt.Errorf("cannot find the file format in the output %s", output)
}

func ConvertFromRawToQcow2(filePath string) error {
	if format, err := DetectFileFormat(filePath); err != nil {
		return err
	} else if format == "qcow2" {
		return nil
	}

	tmpFilePath := filePath + ".qcow2tmp"
	defer os.RemoveAll(tmpFilePath)

	if _, err := Execute([]string{}, QemuImgBinary, "convert", "-f", "raw", "-O", "qcow2", filePath, tmpFilePath); err != nil {
		return err
	}
	if err := os.RemoveAll(filePath); err != nil {
		return err
	}
	return os.Rename(tmpFilePath, filePath)
}
