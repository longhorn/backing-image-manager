package server

import (
	"fmt"
	"net/http"
	"sync"

	"golang.org/x/net/context"

	"github.com/longhorn/sparse-tools/sparse"
	sparserest "github.com/longhorn/sparse-tools/sparse/rest"

	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

type DownloaderFactory interface {
	NewDownloader() Downloader
}

type Downloader interface {
	GetSize(url string) (size int64, err error)
	Pull(url, filepath string, updater util.ProgressUpdater) (written int64, err error)
	Receive(port string, filePath string, syncFileOps sparserest.SyncFileOperations) error
	Send(filePath string, address string) error
	Cancel()
}

type BackingImageDownloaderFactory struct{}

type BackingImageDownloader struct {
	*sync.RWMutex
	cancelFunc context.CancelFunc
}

func (f *BackingImageDownloaderFactory) NewDownloader() Downloader {
	return &BackingImageDownloader{
		&sync.RWMutex{},
		nil,
	}
}

func (d *BackingImageDownloader) GetSize(url string) (written int64, err error) {
	return util.GetDownloadSize(url)
}

func (d *BackingImageDownloader) Pull(url, filepath string, updater util.ProgressUpdater) (written int64, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	d.Lock()
	d.cancelFunc = cancel
	d.Unlock()

	defer func() {
		d.Cancel()
	}()
	return util.DownloadFile(ctx, cancel, url, filepath, updater)
}

func (d *BackingImageDownloader) Receive(port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	_, cancel := context.WithCancel(context.Background())
	d.Lock()
	d.cancelFunc = cancel
	d.Unlock()

	defer func() {
		d.Cancel()
	}()

	if err := sparserest.Server(port, filePath, syncFileOps); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Send should fail once the receiver is closed or the timeout is reached.
func (d *BackingImageDownloader) Send(filePath string, address string) error {
	if d.isDownloading() {
		return fmt.Errorf("downloader cannot send files when the pulling or receiving is still in progress")
	}

	return sparse.SyncFile(filePath, address, types.FileSyncTimeout, false)
}

func (d *BackingImageDownloader) Cancel() {
	d.Lock()
	defer d.Unlock()
	if d.cancelFunc != nil {
		d.cancelFunc()
		d.cancelFunc = nil
	}
}

func (d *BackingImageDownloader) isDownloading() bool {
	d.Lock()
	defer d.Unlock()
	return d.cancelFunc != nil
}
