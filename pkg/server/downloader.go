package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/longhorn/sparse-tools/sparse"
	sparserest "github.com/longhorn/sparse-tools/sparse/rest"

	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
	"github.com/longhorn/backing-image-manager/pkg/util/uploadserver"
)

type BackingImageDownloader struct {
	*sync.RWMutex
	isDownloading bool
	ctx           context.Context
	cancelFunc    context.CancelFunc

	// for unit test
	DownloaderEngine
}

type DownloaderEngine interface {
	GetSize(url string) (int64, error)
	Download(ctx context.Context, url, filePath string, updater util.ProgressUpdater) (written int64, err error)
	ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error
	SenderLaunch(localPath string, remote string, timeout int, directIO bool) error
	UploadServerLaunch(ctx context.Context, port string, directory string, syncFileOps sparserest.SyncFileOperations, sizeUpdater uploadserver.SizeUpdater) error
}

func (d *BackingImageDownloader) GetSize(url string) (int64, error) {
	return d.DownloaderEngine.GetSize(url)
}

func (d *BackingImageDownloader) InitDownloading() error {
	d.Lock()
	defer d.Unlock()
	if d.ctx != nil && d.cancelFunc != nil {
		return fmt.Errorf("downloader is already initialized")
	}
	if d.ctx != nil || d.cancelFunc != nil {
		d.cancelDownloadingWithoutLock()
		return fmt.Errorf("downloader is not initialized correctly")
	}
	d.ctx, d.cancelFunc = context.WithCancel(context.Background())
	return nil
}

func (d *BackingImageDownloader) Pull(url, filePath string, updater util.ProgressUpdater) (written int64, err error) {
	d.Lock()
	if d.isDownloading {
		d.Unlock()
		return 0, fmt.Errorf("downloader is already downloading")
	}
	if d.ctx == nil || d.cancelFunc == nil {
		d.cancelDownloadingWithoutLock()
		d.Unlock()
		return 0, fmt.Errorf("BUG: downloader is not initialized correctly or already cancelled before actual pulling")
	}
	ctx := d.ctx
	d.isDownloading = true
	d.Unlock()

	defer func() {
		d.Cancel()
	}()
	return d.DownloaderEngine.Download(ctx, url, filePath, updater)
}

func (d *BackingImageDownloader) Receive(port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	d.Lock()
	if d.isDownloading {
		d.Unlock()
		return fmt.Errorf("downloader is already downloading")
	}
	if d.ctx == nil || d.cancelFunc == nil {
		d.cancelDownloadingWithoutLock()
		d.Unlock()
		return fmt.Errorf("BUG: downloader is not initialized correctly or already cancelled before actual receiving")
	}
	ctx := d.ctx
	d.isDownloading = true
	d.Unlock()

	defer func() {
		d.Cancel()
	}()

	if err := d.DownloaderEngine.ReceiverLaunch(ctx, port, filePath, syncFileOps); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Send should fail once the receiver is closed or the timeout is reached.
func (d *BackingImageDownloader) Send(filePath string, address string) error {
	if d.isDownloadingInitialized() {
		return fmt.Errorf("downloader cannot send files when the pulling or receiving is still in progress")
	}

	return d.DownloaderEngine.SenderLaunch(filePath, address, types.FileSyncTimeout, false)
}

func (d *BackingImageDownloader) Upload(port string, directory string, syncFileOps sparserest.SyncFileOperations, sizeUpdater uploadserver.SizeUpdater) error {
	d.Lock()
	if d.isDownloading {
		d.Unlock()
		return fmt.Errorf("downloader is already downloading/uploading")
	}
	if d.ctx == nil || d.cancelFunc == nil {
		d.cancelDownloadingWithoutLock()
		d.Unlock()
		return fmt.Errorf("BUG: downloader is not initialized correctly or already cancelled before actual uploading")
	}
	ctx := d.ctx
	d.isDownloading = true
	d.Unlock()

	defer func() {
		d.Cancel()
	}()

	if err := d.DownloaderEngine.UploadServerLaunch(ctx, port, directory, syncFileOps, sizeUpdater); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (d *BackingImageDownloader) Cancel() {
	d.Lock()
	defer d.Unlock()
	d.cancelDownloadingWithoutLock()
}

func (d *BackingImageDownloader) cancelDownloadingWithoutLock() {
	d.isDownloading = false
	d.ctx = nil
	if d.cancelFunc != nil {
		d.cancelFunc()
		d.cancelFunc = nil
	}
}

func (d *BackingImageDownloader) isDownloadingInitialized() bool {
	d.RLock()
	defer d.RUnlock()
	return d.ctx != nil || d.cancelFunc != nil
}

type DownloaderFactory interface {
	NewDownloader() *BackingImageDownloader
}

type BackingImageDownloaderFactory struct{}

type BackingImageDownloaderEngine struct{}

func (e *BackingImageDownloaderEngine) GetSize(url string) (int64, error) {
	return util.GetDownloadSize(url)
}

func (e *BackingImageDownloaderEngine) Download(ctx context.Context, url, filePath string, updater util.ProgressUpdater) (written int64, err error) {
	return util.DownloadFile(ctx, url, filePath, updater)
}

func (e *BackingImageDownloaderEngine) ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	return sparserest.Server(ctx, port, filePath, syncFileOps)
}

func (e *BackingImageDownloaderEngine) SenderLaunch(localPath string, remote string, timeout int, directIO bool) error {
	return sparse.SyncFile(localPath, remote, timeout, directIO)
}

func (e *BackingImageDownloaderEngine) UploadServerLaunch(ctx context.Context, port string, directory string, syncFileOps sparserest.SyncFileOperations, sizeUpdater uploadserver.SizeUpdater) error {
	return uploadserver.NewUploadServer(ctx, port, directory, syncFileOps, sizeUpdater)
}

func (f *BackingImageDownloaderFactory) NewDownloader() *BackingImageDownloader {
	return &BackingImageDownloader{
		&sync.RWMutex{},
		false, nil, nil,
		&BackingImageDownloaderEngine{},
	}
}

type MockDownloaderFactory struct{}

type MockDownloaderEngine struct{}

func (e *MockDownloaderEngine) GetSize(url string) (int64, error) {
	return MockDownloadSize, nil
}

func (e *MockDownloaderEngine) Download(ctx context.Context, url, filePath string, updater util.ProgressUpdater) (written int64, err error) {
	return MockDownloadSize, e.mockDownload(ctx, filePath, updater.UpdateSyncFileProgress)
}

func (e *MockDownloaderEngine) ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	return e.mockDownload(ctx, filePath, syncFileOps.UpdateSyncFileProgress)
}

func (e *MockDownloaderEngine) SenderLaunch(localPath string, remote string, timeout int, directIO bool) error {
	return nil
}

func (e *MockDownloaderEngine) UploadServerLaunch(ctx context.Context, port string, directory string, syncFileOps sparserest.SyncFileOperations, sizeUpdater uploadserver.SizeUpdater) error {
	// No need to mock upload server here
	return uploadserver.NewUploadServer(ctx, port, directory, syncFileOps, sizeUpdater)
}

func (e *MockDownloaderEngine) mockDownload(ctx context.Context, filePath string, progressUpdateFunc func(int64)) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	f.Close()
	if err := os.Truncate(filePath, MockDownloadSize); err != nil {
		return err
	}

	for i := 1; i <= MockDownloadSize; i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled mock receiving")
		default:
			progressUpdateFunc(1)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

const MockDownloadSize = 100

func (f *MockDownloaderFactory) NewDownloader() *BackingImageDownloader {
	return &BackingImageDownloader{
		&sync.RWMutex{},
		false, nil, nil,
		&MockDownloaderEngine{},
	}
}
