package main

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backing-image-manager/app/cmd"
	"github.com/longhorn/backing-image-manager/pkg/meta"
)

// following variables will be filled by `-ldflags "-X ..."`
var (
	Version   string
	GitCommit string
	BuildDate string
)

func main() {
	a := cli.NewApp()

	a.Version = Version
	meta.Version = Version
	meta.GitCommit = GitCommit
	meta.BuildDate = BuildDate

	a.Before = func(c *cli.Context) error {
		if c.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		return nil
	}
	a.Flags = []cli.Flag{
		cli.BoolFlag{
			Name: "debug",
		},
	}
	a.Commands = []cli.Command{
		cmd.StartCmd(),
		cmd.BackingImageCmd(),
		cmd.DataSourceCmd(),
		VersionCmd(),
	}
	if err := a.Run(os.Args); err != nil {
		logrus.Fatal("Error when executing command: ", err)
	}
}
