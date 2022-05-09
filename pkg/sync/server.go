package sync

import (
	"context"
	"net/http"

	"github.com/sirupsen/logrus"
)

func NewServer(parentCtx context.Context, listenAddr string, handler Handler) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	srv := &http.Server{
		Addr: listenAddr,
	}
	service, err := InitService(ctx, listenAddr, handler)
	if err != nil {
		return err
	}
	srv.Handler = NewRouter(service)

	go func() {
		<-ctx.Done()
		if err := srv.Close(); err != nil {
			panic(err)
		}
		logrus.Info("Closed sync server")
	}()

	logrus.Infof("Started sync server at %v", listenAddr)

	return srv.ListenAndServe()
}
