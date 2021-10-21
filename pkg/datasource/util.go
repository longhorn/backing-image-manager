package datasource

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/longhorn/backing-image-manager/pkg/types"
)

const (
	DownloadBufferSize = 1 << 12
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

	copied, err := IdleTimeoutCopy(ctx, cancel, resp.Body, outFile, updater)
	if err != nil {
		return 0, err
	}

	if err := outFile.Truncate(copied); err != nil {
		return 0, errors.Wrapf(err, "failed to truncate the file after download")
	}

	return copied, nil
}

func IdleTimeoutCopy(ctx context.Context, cancel context.CancelFunc, src io.Reader, dst io.WriteSeeker, updater ProgressUpdater) (copied int64, err error) {
	writeSeekCh := make(chan int64)

	go func() {
		t := time.NewTimer(types.HTTPTimeout)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				cancel()
			case <-writeSeekCh:
				if !t.Stop() {
					<-t.C
				}
				t.Reset(types.HTTPTimeout)
			}
		}
	}()

	var nr, nw int
	var nws int64
	var rErr, handleErr error
	buf := make([]byte, DownloadBufferSize)
	zeroByteArray := make([]byte, DownloadBufferSize)
	for {
		// Read will error out once the context is cancelled.
		nr, rErr = src.Read(buf)
		if nr > 0 {
			// Skip writing zero data
			if bytes.Equal(buf[0:nr], zeroByteArray[0:nr]) {
				_, handleErr = dst.Seek(int64(nr), io.SeekCurrent)
				nws = int64(nr)
			} else {
				nw, handleErr = dst.Write(buf[0:nr])
				nws = int64(nw)
			}
			if handleErr != nil {
				err = handleErr
				break
			}
			writeSeekCh <- nws
			copied += nws
			updater.UpdateProgress(nws)
		}
		if rErr != nil {
			if rErr != io.EOF {
				err = rErr
			}
			break
		}
	}
	return copied, err
}
