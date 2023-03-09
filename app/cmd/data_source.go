package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backing-image-manager/pkg/datasource"
	"github.com/longhorn/backing-image-manager/pkg/sync"
	"github.com/longhorn/backing-image-manager/pkg/types"
)

func DataSourceCmd() cli.Command {
	return cli.Command{
		Name: "data-source",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:" + strconv.Itoa(types.DefaultDataSourceServerPort),
				Usage: "Specify the data source server endpoint to listen on host:port. Defaults to localhost:8000",
			},
			cli.StringFlag{
				Name:  "sync-listen",
				Value: "localhost:" + strconv.Itoa(types.DefaultSyncServerPort),
				Usage: "Specify the sync server endpoint to listen on host:port. Defaults to localhost:8001",
			},
			cli.StringFlag{
				Name:  "name",
				Usage: "The name of the backing image",
			},
			cli.StringFlag{
				Name:  "uuid",
				Usage: "The uuid of the backing image",
			},
			cli.StringFlag{
				Name:  "source-type",
				Usage: "There are 3 ways to prepare a backing image file data: download, upload and export-from-volume",
			},
			cli.StringSliceFlag{
				Name:  "parameters",
				Usage: "Parameters for backing image of different source type.",
			},
			cli.StringFlag{
				Name:  "checksum",
				Value: "",
				Usage: "The SHA512 checksum of the backing images",
			},
		},
		Action: func(c *cli.Context) {
			if err := dataSource(c); err != nil {
				logrus.WithError(err).Fatalf("Error running data-source command")
			}
		},
	}
}

func dataSource(c *cli.Context) error {
	logrus.SetLevel(logrus.DebugLevel)

	listen := c.String("listen")
	syncListen := c.String("sync-listen")
	name := c.String("name")
	uuid := c.String("uuid")
	sourceType := c.String("source-type")
	checksum := c.String("checksum")
	parameters, err := parseSliceToMap(c.StringSlice("parameters"))
	if err != nil {
		return err
	}

	return datasource.NewServer(context.Background(), listen, syncListen, checksum, sourceType, name, uuid, types.DiskPathInContainer, parameters, &sync.HTTPHandler{})
}

func parseSliceToMap(sli []string) (map[string]string, error) {
	res := map[string]string{}
	for _, s := range sli {
		kvPair := strings.Split(s, "=")
		if len(kvPair) != 2 {
			return nil, fmt.Errorf("invalid slice input %v since it cannot be converted to a map entry", kvPair)
		}
		if kvPair[0] == "" {
			return nil, fmt.Errorf("invalid slice input %v due to the empty key", kvPair)
		}
		res[kvPair[0]] = kvPair[1]
	}
	return res, nil
}
