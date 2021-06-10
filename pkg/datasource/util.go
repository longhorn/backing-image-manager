package datasource

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/longhorn/backing-image-manager/pkg/types"
)

const (
	DownloadBufferSize = 2 << 13
)

type ProgressUpdater interface {
	UpdateProgress(size int64)
}

type HTTPDownloader interface {
	GetDownloadSize(url string) (fileSize int64, err error)
	DownloadFile(ctx context.Context, url, filepath string, updater ProgressUpdater) (written int64, err error)
}

type Downloader struct{}

func (d *Downloader) GetDownloadSize(url string) (fileSize int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), types.HTTPTimeout)
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

func (d *Downloader) DownloadFile(ctx context.Context, url, filepath string, updater ProgressUpdater) (written int64, err error) {
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

	return IdleTimeoutCopy(ctx, cancel, resp.Body, outFile, updater)
}

func IdleTimeoutCopy(ctx context.Context, cancel context.CancelFunc, src io.Reader, dst io.Writer, updater ProgressUpdater) (written int64, err error) {
	writeCh := make(chan int)

	go func() {
		t := time.NewTimer(types.HTTPTimeout)
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
				t.Reset(types.HTTPTimeout)
			}
		}
	}()

	buf := make([]byte, DownloadBufferSize)
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
			updater.UpdateProgress(int64(nw))
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
