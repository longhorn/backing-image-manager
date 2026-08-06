package cmd

import (
	"context"
	"fmt"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

func BackingImageCmd() *cli.Command {
	return &cli.Command{
		Name: "backing-image",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "url",
				Value: "localhost:" + strconv.Itoa(types.DefaultManagerPort),
				Usage: "Specify the manager server endpoint to listen on host:port. Defaults to localhost:8000",
			},
		},
		Commands: []*cli.Command{
			SyncCmd(),
			SendCmd(),
			DeleteCmd(),
			GetCmd(),
			ListCmd(),
			FetchCmd(),
			PrepareDownloadCmd(),
		},
	}
}

func SyncCmd() *cli.Command {
	return &cli.Command{
		Name: "file-sync",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "name",
				Usage: "The name of the backing image to be synced",
			},
			&cli.StringFlag{
				Name:  "uuid",
				Usage: "The uuid of the backing image to be synced",
			},
			&cli.Int64Flag{
				Name:  "size",
				Usage: "The size of the backing images to be synced",
			},
			&cli.StringFlag{
				Name:  "from-address",
				Usage: "Backing image manager address, will request sending backing image from the address",
			},
			&cli.StringFlag{
				Name:  "checksum",
				Value: "",
				Usage: "The SHA512 checksum of the backing images to be synced",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := fileSync(c); err != nil {
				logrus.WithError(err).Fatalf("Error running backing image file-sync command")
			}
			return nil
		},
	}
}

func fileSync(c *cli.Command) error {
	url := c.String("url")
	bimClient := client.NewBackingImageManagerClient(url)
	bi, err := bimClient.Sync(c.String("name"), c.String("uuid"), c.String("checksum"), c.String("from-address"), c.Int64("size"))
	if err != nil {
		return err
	}
	return util.PrintJSON(bi)
}

func SendCmd() *cli.Command {
	return &cli.Command{
		Name: "send",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "name",
				Usage: "The name of the backing image",
			},
			&cli.StringFlag{
				Name:  "uuid",
				Usage: "The name of the backing image",
			},
			&cli.StringFlag{
				Name:  "to-address",
				Usage: "Backing image manager address, will send backing image to this address",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := send(c); err != nil {
				logrus.WithError(err).Fatalf("Error running backing image send command")
			}
			return nil
		},
	}
}

func send(c *cli.Command) error {
	url := c.String("url")
	bimClient := client.NewBackingImageManagerClient(url)
	return bimClient.Send(c.String("name"), c.String("uuid"), c.String("to-address"))
}

func DeleteCmd() *cli.Command {
	return &cli.Command{
		Name: "delete",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "name",
				Usage: "The name of the backing image",
			},
			&cli.StringFlag{
				Name:  "uuid",
				Usage: "The name of the backing image",
			},
		},
		Aliases: []string{"del"},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := del(c); err != nil {
				logrus.WithError(err).Fatalf("Error running backing image delete command")
			}
			return nil
		},
	}
}

func del(c *cli.Command) error {
	url := c.String("url")
	bimClient := client.NewBackingImageManagerClient(url)
	return bimClient.Delete(c.String("name"), c.String("uuid"))
}

func GetCmd() *cli.Command {
	return &cli.Command{
		Name: "get",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "name",
				Usage: "The name of the backing image",
			},
			&cli.StringFlag{
				Name:  "uuid",
				Usage: "The name of the backing image",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := get(c); err != nil {
				logrus.WithError(err).Fatalf("Error running backing image get command")
			}
			return nil
		},
	}
}

func get(c *cli.Command) error {
	url := c.String("url")
	bimClient := client.NewBackingImageManagerClient(url)
	bi, err := bimClient.Get(c.String("name"), c.String("uuid"))
	if err != nil {
		return err
	}
	return util.PrintJSON(bi)
}

func ListCmd() *cli.Command {
	return &cli.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := list(c); err != nil {
				logrus.WithError(err).Fatalf("Error running backing image list command")
			}
			return nil
		},
	}
}

func list(c *cli.Command) error {
	url := c.String("url")
	bimClient := client.NewBackingImageManagerClient(url)
	biList, err := bimClient.List()
	if err != nil {
		return err
	}
	return util.PrintJSON(biList)
}

func FetchCmd() *cli.Command {
	return &cli.Command{
		Name: "sync",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "name",
				Usage: "The name of the backing image",
			},
			&cli.StringFlag{
				Name:  "uuid",
				Usage: "The uuid of the backing image",
			},
			&cli.Int64Flag{
				Name:  "size",
				Usage: "The size of the backing image to be fetched",
			},
			&cli.StringFlag{
				Name:  "data-source-address",
				Value: "",
				Usage: "Data source server address, will fetch the file from this server",
			},
			&cli.StringFlag{
				Name:  "checksum",
				Value: "",
				Usage: "The SHA512 checksum of the backing image to be fetched",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := fetch(c); err != nil {
				logrus.WithError(err).Fatalf("Error running backing image fetch command")
			}
			return nil
		},
	}
}

func fetch(c *cli.Command) error {
	url := c.String("url")
	bimClient := client.NewBackingImageManagerClient(url)
	bi, err := bimClient.Fetch(c.String("name"), c.String("uuid"), c.String("checksum"), c.String("data-source-address"), c.Int64("size"))
	if err != nil {
		return err
	}
	return util.PrintJSON(bi)
}

func PrepareDownloadCmd() *cli.Command {
	return &cli.Command{
		Name: "prepare-download",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "name",
				Usage: "The name of the backing image",
			},
			&cli.StringFlag{
				Name:  "uuid",
				Usage: "The uuid of the backing image",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := prepareDownload(c); err != nil {
				logrus.WithError(err).Fatalf("Error running backing image prepare download command")
			}
			return nil
		},
	}
}

func prepareDownload(c *cli.Command) error {
	url := c.String("url")
	bimClient := client.NewBackingImageManagerClient(url)
	srcFilePath, address, err := bimClient.PrepareDownload(c.String("name"), c.String("uuid"))
	if err != nil {
		return err
	}
	fmt.Println("Source file path:", srcFilePath)
	fmt.Println("Download server address:", address)
	return nil
}
