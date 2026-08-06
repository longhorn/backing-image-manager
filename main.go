package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

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
	meta.Version = Version
	meta.GitCommit = GitCommit
	meta.BuildDate = BuildDate

	logrus.SetReportCaller(true)
	logrus.SetFormatter(&logrus.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (function string, file string) {
			fileName := fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
			funcName := path.Base(f.Function)
			return funcName, fileName
		},
		FullTimestamp: true,
	})

	a := &cli.Command{
		Version: Version,
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			if c.Bool("debug") {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return ctx, nil
		},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name: "debug",
			},
		},
		Commands: []*cli.Command{
			cmd.StartCmd(),
			cmd.BackingImageCmd(),
			cmd.DataSourceCmd(),
			VersionCmd(),
		},
	}
	if err := a.Run(context.Background(), os.Args); err != nil {
		logrus.Fatal("Error when executing command: ", err)
	}
}
