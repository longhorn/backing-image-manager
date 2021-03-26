package cmd

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/backing-image-manager/pkg/health"
	"github.com/longhorn/backing-image-manager/pkg/rpc"
	"github.com/longhorn/backing-image-manager/pkg/server"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

func StartCmd() cli.Command {
	return cli.Command{
		Name: "daemon",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:8000",
			},
			cli.StringFlag{
				Name:  "disk-path",
				Value: "/var/lib/longhorn",
				Usage: "The corresponding host disk path of the work directory",
			},
			cli.StringFlag{
				Name:  "disk-uuid",
				Usage: "The corresponding disk uuid stored in the metafile of the disk path",
			},
			cli.StringFlag{
				Name:  "port-range",
				Value: "30001-31000",
			},
		},
		Action: func(c *cli.Context) {
			if err := start(c); err != nil {
				logrus.Fatalf("Error running start command: %v.", err)
			}
		},
	}
}

func start(c *cli.Context) error {
	listen := c.String("listen")
	diskUUID := c.String("disk-uuid")
	diskPathOnHost := c.String("disk-path")
	portRange := c.String("port-range")

	diskUUIDInFile, err := util.GetDiskConfig(types.DiskPath)
	if err != nil {
		return err
	}
	if diskUUID == "" {
		diskUUID = diskUUIDInFile
	} else if diskUUID != diskUUIDInFile {
		return fmt.Errorf("invalid input disk UUID %v, which doesn't match disk UUID %v the disk config file", diskUUID, diskUUIDInFile)
	}

	shutdownCh := make(chan error)
	bim, err := server.NewManager(diskUUID, diskPathOnHost, portRange, shutdownCh)
	if err != nil {
		return err
	}
	hc := health.NewHealthCheckServer(bim)

	listenAt, err := net.Listen("tcp", listen)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	rpcService := grpc.NewServer()
	rpc.RegisterBackingImageManagerServiceServer(rpcService, bim)
	healthpb.RegisterHealthServer(rpcService, hc)
	reflection.Register(rpcService)

	go func() {
		if err := rpcService.Serve(listenAt); err != nil {
			logrus.Errorf("Stopping due to %v:", err)
		}
		// graceful shutdown before exit
		bim.Shutdown()
		close(shutdownCh)
	}()
	logrus.Infof("Backing Image Manager listening to %v", listen)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Backing Image Manager received %v to exit", sig)
		rpcService.Stop()
	}()

	return <-shutdownCh
}
