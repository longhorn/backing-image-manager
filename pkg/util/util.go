package util

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

const (
	HTTPTimeout             = 4 * time.Second
	PreservedChecksumLength = 64

	LetterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func PrintJSON(obj interface{}) error {
	output, err := json.MarshalIndent(obj, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func GetChecksum(data []byte) string {
	checksumBytes := sha512.Sum512(data)
	checksum := hex.EncodeToString(checksumBytes[:])[:PreservedChecksumLength]
	return checksum
}

func RandStringBytes(n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = LetterBytes[rand.Intn(len(LetterBytes))]
	}
	return b
}

func GenerateRandomDataFile(filePath string, sizeInMB int) error {
	// 1Mi
	chunkSize := int64(1 * 1024 * 1024)

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	defer f.Sync()
	if err := f.Truncate(int64(sizeInMB) * chunkSize); err != nil {
		return err
	}

	data := make([]byte, chunkSize)
	for i := 0; i < sizeInMB; i++ {
		data = RandStringBytes(chunkSize)
		if _, err := f.WriteAt(data, int64(i)*chunkSize); err != nil {
			return err
		}
	}

	return nil
}

type ProgressUpdater interface {
	UpdateSyncFileProgress(size int64)
}

func GetDownloadSize(url string) (fileSize int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), HTTPTimeout)
	defer cancel()

	rr, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, err
	}

	client := http.Client{}
	resp, err := client.Do(rr)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("expected status code 200 from %s, got %s", url, resp.Status)
	}

	if resp.Header.Get("Content-Length") == "" {
		// -1 indicates unknown size
		fileSize = -1
	} else {
		fileSize, err = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			return 0, err
		}
	}

	return fileSize, nil
}

func DownloadFile(ctx context.Context, url, filepath string, updater ProgressUpdater) (written int64, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rr, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, err
	}

	client := http.Client{}
	resp, err := client.Do(rr)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("expected status code 200 from %s, got %s", url, resp.Status)
	}

	outFile, err := os.Create(filepath)
	if err != nil {
		return 0, err
	}
	defer outFile.Close()

	return idleTimeoutCopy(ctx, cancel, resp.Body, outFile, updater)
}

func idleTimeoutCopy(ctx context.Context, cancel context.CancelFunc, src io.Reader, dst io.Writer, updater ProgressUpdater) (written int64, err error) {
	writeCh := make(chan int)

	go func() {
		t := time.NewTimer(HTTPTimeout)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				cancel()
			case <-writeCh:
				if !t.Stop() {
					<-t.C
				}
				t.Reset(HTTPTimeout)
			}
		}
	}()

	buf := make([]byte, 512*1024)
	for {
		// Read will error out once the context is cancelled.
		nr, readErr := src.Read(buf)
		if nr > 0 {
			nw, writeErr := dst.Write(buf[0:nr])
			if writeErr != nil {
				err = writeErr
				break
			}
			writeCh <- nw
			written += int64(nw)
			updater.UpdateSyncFileProgress(int64(nw))
		}
		if readErr != nil {
			if readErr != io.EOF {
				err = readErr
			}
			break
		}
	}
	return written, err
}

// This should be the same as the schema in longhorn-manager/util
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
	Name string `json:"name"`
	UUID string `json:"uuid"`
	Size int64  `json:"size"`
	URL  string `json:"url"`
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
