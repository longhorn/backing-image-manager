package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backing-image-manager/pkg/datasource"
	"github.com/longhorn/backing-image-manager/pkg/types"
)

func DataSourceCmd() cli.Command {
	return cli.Command{
		Name: "data-source",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: ":" + strconv.Itoa(types.DefaultDataSourceServerPort),
			},
			cli.StringFlag{
				Name: "file-name",
			},
			cli.StringFlag{
				Name: "source-type",
			},
			cli.StringSliceFlag{
				Name: "parameters",
			},
			cli.StringFlag{
				Name:  "checksum",
				Value: "",
			},
		},
		Action: func(c *cli.Context) {
			if err := dataSource(c); err != nil {
				logrus.Fatalf("Error running data-source command: %v.", err)
			}
		},
	}
}

func dataSource(c *cli.Context) error {
	listen := c.String("listen")
	fileName := c.String("file-name")
	sourceType := c.String("source-type")
	checksum := c.String("checksum")
	parameters, err := parseSliceToMap(c.StringSlice("parameters"))
	if err != nil {
		return err
	}

	return datasource.NewServer(listen, fileName, checksum, sourceType, parameters, types.DiskPathInContainer, &datasource.Downloader{})
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
