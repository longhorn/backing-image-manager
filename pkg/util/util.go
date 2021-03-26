package util

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const HTTPTimeout = 4 * time.Second

func PrintJSON(obj interface{}) error {
	output, err := json.MarshalIndent(obj, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
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

func DownloadFile(url, filepath string, updater ProgressUpdater) (written int64, err error) {
	ctx, cancel := context.WithCancel(context.Background())
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
