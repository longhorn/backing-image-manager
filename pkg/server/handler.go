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
	ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error
	SenderLaunch(localPath string, remote string, timeout int, directIO bool) error
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
	if d.isProcessingInitialized() {
		return fmt.Errorf("handler cannot send files when the pulling or receiving is still in progress")
	}

	return d.HandlerEngine.SenderLaunch(filePath, address, types.FileSyncTimeout, false)
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

func (d *BackingImageHandler) isProcessingInitialized() bool {
	d.RLock()
	defer d.RUnlock()
	return d.ctx != nil || d.cancelFunc != nil
}

type HandlerFactory interface {
	NewHandler() *BackingImageHandler
}

type BackingImageHandlerFactory struct{}

type BackingImageHandlerEngine struct{}

func (e *BackingImageHandlerEngine) ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	return sparserest.Server(ctx, port, filePath, syncFileOps)
}

func (e *BackingImageHandlerEngine) SenderLaunch(localPath string, remote string, timeout int, directIO bool) error {
	return sparse.SyncFile(localPath, remote, timeout, directIO)
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

func (e *MockHandlerEngine) ReceiverLaunch(ctx context.Context, port string, filePath string, syncFileOps sparserest.SyncFileOperations) error {
	return e.mockProcessing(ctx, filePath, syncFileOps.UpdateSyncFileProgress)
}

func (e *MockHandlerEngine) SenderLaunch(localPath string, remote string, timeout int, directIO bool) error {
	return nil
}

func (e *MockHandlerEngine) mockProcessing(ctx context.Context, filePath string, progressUpdateFunc func(int64)) error {
	if err := GenerateTestFile(filePath, MockProcessingSize); err != nil {
		return err
	}

	for i := 1; i <= MockProcessingSize; i++ {
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

const MockProcessingSize = 100

func GenerateTestFile(filePath string, size int64) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(size)
}

func (f *MockHandlerFactory) NewHandler() *BackingImageHandler {
	return &BackingImageHandler{
		&sync.RWMutex{},
		false, nil, nil,
		&MockHandlerEngine{},
	}
}
