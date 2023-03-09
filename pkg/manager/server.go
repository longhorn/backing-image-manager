package manager

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/backing-image-manager/pkg/rpc"
	"github.com/longhorn/backing-image-manager/pkg/sync"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

func NewServer(parentCtx context.Context, listenAddr, syncListenAddr, diskUUID, diskPathInContainer, portRange string, syncHandler sync.Handler) error {
	ctx, cancel := context.WithCancel(parentCtx)

	// TODO: May launch the sync service separately
	go func() {
		if err := sync.NewServer(ctx, syncListenAddr, syncHandler); err != nil {
			logrus.Warnf("File sync service errored out: %v", err)
		}
		cancel()
	}()
	if !util.DetectHTTPServerAvailability("http://"+syncListenAddr, 5, true) {
		return fmt.Errorf("failed to wait for sync service running in 5 second")
	}
	logrus.Infof("The sync server of backing Image Manager listening to %v", syncListenAddr)

	listenAt, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	bim, err := NewManager(ctx, syncListenAddr, diskUUID, diskPathInContainer, portRange)
	if err != nil {
		return err
	}
	rpcService := grpc.NewServer()
	rpc.RegisterBackingImageManagerServiceServer(rpcService, bim)
	reflection.Register(rpcService)
	go func() {
		if err := rpcService.Serve(listenAt); err != nil {
			logrus.WithError(err).Errorf("stopping the server due to there is an error")
		}
		// graceful shutdown before exit
		cancel()
	}()
	logrus.Infof("Backing Image Manager listening to %v", listenAddr)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Backing Image Manager received %v to exit", sig)
		rpcService.Stop()
	}()

	<-ctx.Done()
	return nil
}
