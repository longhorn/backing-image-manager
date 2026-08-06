package main

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/meta"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

func VersionCmd() *cli.Command {
	return &cli.Command{
		Name: "version",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name: "client-only",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := version(c); err != nil {
				logrus.Fatalln("Error running info command:", err)
			}
			return nil
		},
	}
}

type VersionOutput struct {
	ClientVersion *meta.VersionOutput `json:"clientVersion"`
	ServerVersion *meta.VersionOutput `json:"serverVersion"`
}

func version(c *cli.Command) error {
	clientVersion := meta.GetVersion()
	v := VersionOutput{ClientVersion: &clientVersion}

	if !c.Bool("client-only") {
		url := c.String("url")
		cli := client.NewBackingImageManagerClient(url)
		version, err := cli.VersionGet()
		if err != nil {
			return err
		}
		v.ServerVersion = version
	}
	return util.PrintJSON(v)
}
