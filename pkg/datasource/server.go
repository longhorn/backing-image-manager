package datasource

import (
	"context"
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/pkg/sync"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

func NewServer(parentCtx context.Context, listenAddr, syncListenAddr, checksum, sourceType, biName, biUUID, diskPathInContainer string, parameters map[string]string, handler sync.Handler) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	srv := &http.Server{
		Addr: listenAddr,
	}

	// TODO: Will launch the sync service separately
	go func() {
		if err := sync.NewServer(ctx, syncListenAddr, handler); err != nil {
			logrus.Warnf("File sync service errored out: %v", err)
		}
	}()
	// Wait for Sync Service up and running
	if !util.DetectHTTPServerAvailability("http://"+syncListenAddr, 5, true) {
		return fmt.Errorf("failed to wait for sync service running in 5 second")
	}

	service, err := LaunchService(ctx, cancel, syncListenAddr, checksum, sourceType, biName, biUUID, diskPathInContainer, parameters)
	if err != nil {
		return err
	}
	srv.Handler = NewRouter(service)

	go func() {
		<-ctx.Done()
		if err := srv.Close(); err != nil {
			panic(err)
		}
		logrus.Info("Closed data source server")
	}()

	logrus.Infof("Started data source server at %v", listenAddr)

	return srv.ListenAndServe()
}
