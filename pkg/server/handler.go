package server

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/longhorn/sparse-tools/sparse"
	sparserest "github.com/longhorn/sparse-tools/sparse/rest"

	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
	"github.com/longhorn/backing-image-manager/pkg/util/uploadserver"
)

type BackingImageHandler struct {
	*sync.RWMutex
	isProcessing bool
	ctx          context.Context
	cancelFunc   context.CancelFunc

	// for unit test
	HandlerEngine
}

type HandlerEngine interface {
	GetSize(url string) (int64, error)
	Download(ctx context.Context, url, filePath string, updater util.ProgressUpdater) (written int64, err error)
	ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error
	SenderLaunch(localPath string, remote string, timeout int, directIO bool) error
	UploadServerLaunch(ctx context.Context, port string, directory string, syncFileOps sparserest.SyncFileOperations, sizeUpdater uploadserver.SizeUpdater) error
}

func (d *BackingImageHandler) GetSize(url string) (int64, error) {
	return d.HandlerEngine.GetSize(url)
}

func (d *BackingImageHandler) InitProcessing() error {
	d.Lock()
	defer d.Unlock()
	if d.ctx != nil && d.cancelFunc != nil {
		return fmt.Errorf("handler is already initialized")
	}
	if d.ctx != nil || d.cancelFunc != nil {
		d.cancelProcessingWithoutLock()
		return fmt.Errorf("handler is not initialized correctly")
	}
	d.ctx, d.cancelFunc = context.WithCancel(context.Background())
	return nil
}

func (d *BackingImageHandler) Pull(url, filePath string, updater util.ProgressUpdater) (written int64, err error) {
	d.Lock()
	if d.isProcessing {
		d.Unlock()
		return 0, fmt.Errorf("handler is already processing")
	}
	if d.ctx == nil || d.cancelFunc == nil {
		d.cancelProcessingWithoutLock()
		d.Unlock()
		return 0, fmt.Errorf("BUG: handler is not initialized correctly or already cancelled before actual pulling")
	}
	ctx := d.ctx
	d.isProcessing = true
	d.Unlock()

	defer func() {
		d.Cancel()
	}()
	return d.HandlerEngine.Download(ctx, url, filePath, updater)
}

func (d *BackingImageHandler) Receive(port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	d.Lock()
	if d.isProcessing {
		d.Unlock()
		return fmt.Errorf("handler is already processing")
	}
	if d.ctx == nil || d.cancelFunc == nil {
		d.cancelProcessingWithoutLock()
		d.Unlock()
		return fmt.Errorf("BUG: handler is not initialized correctly or already cancelled before actual receiving")
	}
	ctx := d.ctx
	d.isProcessing = true
	d.Unlock()

	defer func() {
		d.Cancel()
	}()

	if err := d.HandlerEngine.ReceiverLaunch(ctx, port, filePath, syncFileOps); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Send should fail once the receiver is closed or the timeout is reached.
func (d *BackingImageHandler) Send(filePath string, address string) error {
	if d.isProcessingStart() {
		return fmt.Errorf("handler cannot send files when the processing is not finished")
	}

	return d.HandlerEngine.SenderLaunch(filePath, address, types.FileSyncTimeout, false)
}

func (d *BackingImageHandler) Upload(port string, directory string, syncFileOps sparserest.SyncFileOperations, sizeUpdater uploadserver.SizeUpdater) error {
	d.Lock()
	if d.isProcessing {
		d.Unlock()
		return fmt.Errorf("handler is already processing")
	}
	if d.ctx == nil || d.cancelFunc == nil {
		d.cancelProcessingWithoutLock()
		d.Unlock()
		return fmt.Errorf("BUG: handler is not initialized correctly or already cancelled before actual uploading")
	}
	ctx := d.ctx
	d.isProcessing = true
	d.Unlock()

	defer func() {
		d.Cancel()
	}()

	if err := d.HandlerEngine.UploadServerLaunch(ctx, port, directory, syncFileOps, sizeUpdater); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (d *BackingImageHandler) Cancel() {
	d.Lock()
	defer d.Unlock()
	d.cancelProcessingWithoutLock()
}

func (d *BackingImageHandler) cancelProcessingWithoutLock() {
	d.isProcessing = false
	d.ctx = nil
	if d.cancelFunc != nil {
		d.cancelFunc()
		d.cancelFunc = nil
	}
}

func (d *BackingImageHandler) isProcessingStart() bool {
	d.RLock()
	defer d.RUnlock()
	return d.ctx != nil || d.cancelFunc != nil
}

type HandlerFactory interface {
	NewHandler() *BackingImageHandler
}

type BackingImageHandlerFactory struct{}

type BackingImageHandlerEngine struct{}

func (e *BackingImageHandlerEngine) GetSize(url string) (int64, error) {
	return util.GetDownloadSize(url)
}

func (e *BackingImageHandlerEngine) Download(ctx context.Context, url, filePath string, updater util.ProgressUpdater) (written int64, err error) {
	return util.DownloadFile(ctx, url, filePath, updater)
}

func (e *BackingImageHandlerEngine) ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	return sparserest.Server(ctx, port, filePath, syncFileOps)
}

func (e *BackingImageHandlerEngine) SenderLaunch(localPath string, remote string, timeout int, directIO bool) error {
	return sparse.SyncFile(localPath, remote, timeout, directIO)
}

func (e *BackingImageHandlerEngine) UploadServerLaunch(ctx context.Context, port string, directory string, syncFileOps sparserest.SyncFileOperations, sizeUpdater uploadserver.SizeUpdater) error {
	return uploadserver.NewUploadServer(ctx, port, directory, syncFileOps, sizeUpdater)
}

func (f *BackingImageHandlerFactory) NewHandler() *BackingImageHandler {
	return &BackingImageHandler{
		&sync.RWMutex{},
		false, nil, nil,
		&BackingImageHandlerEngine{},
	}
}

type MockHandlerFactory struct{}

type MockHandlerEngine struct{}

func (e *MockHandlerEngine) GetSize(url string) (int64, error) {
	return MockFileSize, nil
}

func (e *MockHandlerEngine) Download(ctx context.Context, url, filePath string, updater util.ProgressUpdater) (written int64, err error) {
	return MockFileSize, e.mockProcessing(ctx, filePath, updater.UpdateSyncFileProgress)
}

func (e *MockHandlerEngine) ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	return e.mockProcessing(ctx, filePath, syncFileOps.UpdateSyncFileProgress)
}

func (e *MockHandlerEngine) SenderLaunch(localPath string, remote string, timeout int, directIO bool) error {
	return nil
}

func (e *MockHandlerEngine) UploadServerLaunch(ctx context.Context, port string, directory string, syncFileOps sparserest.SyncFileOperations, sizeUpdater uploadserver.SizeUpdater) error {
	// No need to mock upload server here
	return uploadserver.NewUploadServer(ctx, port, directory, syncFileOps, sizeUpdater)
}

func (e *MockHandlerEngine) mockProcessing(ctx context.Context, filePath string, progressUpdateFunc func(int64)) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	f.Close()
	if err := os.Truncate(filePath, MockFileSize); err != nil {
		return err
	}

	for i := 1; i <= MockFileSize; i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled mock processing")
		default:
			progressUpdateFunc(1)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

const MockFileSize = 100

func (f *MockHandlerFactory) NewHandler() *BackingImageHandler {
	return &BackingImageHandler{
		&sync.RWMutex{},
		false, nil, nil,
		&MockHandlerEngine{},
	}
}
