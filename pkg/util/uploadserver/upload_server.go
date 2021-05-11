package uploadserver

import (
	"context"
	"net/http"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/pkg/util"
)

type SizeUpdater interface {
	SetUploadSize(size int64)
}

type UploadServer struct {
	directory    string
	size         int64
	uploadedSize int64

	progressUpdater util.ProgressUpdater
	sizeUpdater     SizeUpdater
	ctx             context.Context
	cancelFunc      context.CancelFunc
	log             logrus.FieldLogger

	srv *http.Server

	*sync.Mutex
}

type progressUpdateStub struct {
	size         int64
	uploadedSize int64
}

func (p *progressUpdateStub) UpdateSyncFileProgress(size int64) {
	p.uploadedSize += size
}

func (p *progressUpdateStub) SetUploadSize(size int64) {
	p.size = size
}

func TestUploadServer(ctx context.Context, port string, directory string, progressUpdater util.ProgressUpdater, sizeUpdater SizeUpdater) {
	NewUploadServer(ctx, port, directory, progressUpdater, sizeUpdater)
}

func NewUploadServer(ctx context.Context, port string, directory string, progressUpdater util.ProgressUpdater, sizeUpdater SizeUpdater) error {
	logrus.Infof("Launching upload server")
	ctx, cancelFunc := context.WithCancel(ctx)
	srv := &http.Server{
		Addr: ":" + port,
	}
	syncServer := &UploadServer{
		directory: directory,
		size:      0,

		progressUpdater: progressUpdater,
		sizeUpdater:     sizeUpdater,
		ctx:             ctx,
		cancelFunc:      cancelFunc,
		log:             logrus.StandardLogger().WithField("directory", directory),

		srv:   srv,
		Mutex: &sync.Mutex{},
	}
	srv.Handler = NewRouter(syncServer)

	go func() {
		<-ctx.Done()
		srv.Close()
		logrus.Infof("Closed upload server at port %v", port)
	}()

	return srv.ListenAndServe()
}
