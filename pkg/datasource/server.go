package datasource

import (
	"context"
	"net/http"

	"github.com/sirupsen/logrus"
)

func NewServer(listenAddr, fileName, checksum, sourceType string, parameters map[string]string, diskPathInContainer string, downloader HTTPDownloader) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := &http.Server{
		Addr: listenAddr,
	}
	service, err := LaunchService(ctx, fileName, checksum, sourceType, parameters, diskPathInContainer, downloader)
	if err != nil {
		return err
	}
	srv.Handler = NewRouter(service)

	go func() {
		defer func() {
			srv.Close()
			logrus.Info("Closed data source server")
		}()

		select {
		case <-ctx.Done():
			return
		}
	}()

	logrus.Infof("Starting data source server at %v", listenAddr)

	return srv.ListenAndServe()
}
