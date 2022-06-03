package sync

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

type Handler interface {
	GetSizeFromURL(url string) (fileSize int64, err error)
	DownloadFromURL(ctx context.Context, url, filePath string, updater ProgressUpdater) (written int64, err error)
}

type HTTPHandler struct{}

func (h *HTTPHandler) GetSizeFromURL(url string) (size int64, err error) {
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
		size = -1
	} else {
		size, err = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			return 0, err
		}
	}

	return size, nil
}

func (h *HTTPHandler) DownloadFromURL(ctx context.Context, url, filePath string, updater ProgressUpdater) (written int64, err error) {
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

	outFile, err := os.Create(filePath)
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

// IdleTimeoutCopy relies on ctx of the reader/src or a separate timer to interrupt the processing.
func IdleTimeoutCopy(ctx context.Context, cancel context.CancelFunc, src io.ReadCloser, dst io.WriteSeeker, updater ProgressUpdater) (copied int64, err error) {
	writeSeekCh := make(chan int64, 100)
	defer close(writeSeekCh)

	go func() {
		t := time.NewTimer(types.HTTPTimeout)
		done := false
		for !done {
			select {
			case <-ctx.Done():
				done = true
			case <-t.C:
				cancel()
				done = true
			case _, writeChOpen := <-writeSeekCh:
				if !writeChOpen {
					done = true
					break
				}
				if !t.Stop() {
					<-t.C
				}
				t.Reset(types.HTTPTimeout)
			}
		}

		// Still need to make sure to clean up the signals in writeSeekCh
		// so that they won't block the below sender.
		for writeChOpen := true; writeChOpen; {
			_, writeChOpen = <-writeSeekCh
		}
	}()

	var nr, nw int
	var nws int64
	var rErr, handleErr error
	buf := make([]byte, DownloadBufferSize)
	zeroByteArray := make([]byte, DownloadBufferSize)
	for rErr == nil && err == nil {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("context cancelled during the copy")
		default:
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
	}

	return copied, err
}

const (
	MockFileSize = 4096
)

type MockHandler struct{}

func (mh *MockHandler) GetSizeFromURL(url string) (fileSize int64, err error) {
	return MockFileSize, nil
}
func (mh *MockHandler) DownloadFromURL(ctx context.Context, url, filePath string, updater ProgressUpdater) (written int64, err error) {
	return mh.mockFile(ctx, filePath, updater)
}

func (mh *MockHandler) mockFile(ctx context.Context, filePath string, updater ProgressUpdater) (written int64, err error) {
	f, err := os.Create(filePath)
	if err != nil {
		return 0, err
	}
	f.Close()
	if err := os.Truncate(filePath, MockFileSize); err != nil {
		return 0, err
	}

	for i := 1; i <= MockFileSize/16; i++ {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("cancelled mock processing")
		default:
			updater.UpdateProgress(16)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return MockFileSize, nil
}
