package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

func BackingImageCmd() cli.Command {
	return cli.Command{
		Name: "backing-image",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "url",
				Value: "localhost:8000",
			},
		},
		Subcommands: []cli.Command{
			SyncCmd(),
			SendCmd(),
			DeleteCmd(),
			GetCmd(),
			ListCmd(),
			FetchCmd(),
		},
	}
}

func SyncCmd() cli.Command {
	return cli.Command{
		Name: "sync",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "uuid",
			},
			cli.Int64Flag{
				Name: "size",
			},
			cli.StringFlag{
				Name: "from-host",
			},
			cli.StringFlag{
				Name: "to-host",
			},
			cli.StringFlag{
				Name:  "checksum",
				Value: "",
			},
		},
		Action: func(c *cli.Context) {
			if err := sync(c); err != nil {
				logrus.Fatalf("Error running backing image sync command: %v.", err)
			}
		},
	}
}

func sync(c *cli.Context) error {
	url := c.GlobalString("url")
	bimClient := client.NewBackingImageManagerClient(url)
	bi, err := bimClient.Sync(c.String("name"), c.String("uuid"), c.String("checksum"), c.String("from-host"), c.String("to-host"), c.Int64("size"))
	if err != nil {
		return err
	}
	return util.PrintJSON(bi)
}

func SendCmd() cli.Command {
	return cli.Command{
		Name: "send",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "to-address",
			},
		},
		Action: func(c *cli.Context) {
			if err := send(c); err != nil {
				logrus.Fatalf("Error running backing image send command: %v.", err)
			}
		},
	}
}

func send(c *cli.Context) error {
	url := c.GlobalString("url")
	bimClient := client.NewBackingImageManagerClient(url)
	return bimClient.Send(c.String("name"), c.String("to-address"))
}

func DeleteCmd() cli.Command {
	return cli.Command{
		Name:    "delete",
		Aliases: []string{"del"},
		Action: func(c *cli.Context) {
			if err := del(c); err != nil {
				logrus.Fatalf("Error running backing image delete command: %v.", err)
			}
		},
	}
}

func del(c *cli.Context) error {
	if len(c.Args()) != 1 {
		return fmt.Errorf("receive only 1 parameter as the requested backing image name")
	}
	url := c.GlobalString("url")
	bimClient := client.NewBackingImageManagerClient(url)
	return bimClient.Delete(c.Args()[0])
}

func GetCmd() cli.Command {
	return cli.Command{
		Name: "get",
		Action: func(c *cli.Context) {
			if err := get(c); err != nil {
				logrus.Fatalf("Error running backing image get command: %v.", err)
			}
		},
	}
}

func get(c *cli.Context) error {
	url := c.GlobalString("url")
	if len(c.Args()) != 1 {
		return fmt.Errorf("receive only 1 parameter as the requested backing image name")
	}
	bimClient := client.NewBackingImageManagerClient(url)
	bi, err := bimClient.Get(c.Args()[0])
	if err != nil {
		return err
	}
	return util.PrintJSON(bi)
}

func ListCmd() cli.Command {
	return cli.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Action: func(c *cli.Context) {
			if err := list(c); err != nil {
				logrus.Fatalf("Error running backing image list command: %v.", err)
			}
		},
	}
}

func list(c *cli.Context) error {
	url := c.GlobalString("url")
	bimClient := client.NewBackingImageManagerClient(url)
	biList, err := bimClient.List()
	if err != nil {
		return err
	}
	return util.PrintJSON(biList)
}

func FetchCmd() cli.Command {
	return cli.Command{
		Name: "sync",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "uuid",
			},
			cli.Int64Flag{
				Name: "size",
			},
			cli.StringFlag{
				Name:  "source-file-name",
				Value: "",
			},
			cli.StringFlag{
				Name:  "checksum",
				Value: "",
			},
		},
		Action: func(c *cli.Context) {
			if err := fetch(c); err != nil {
				logrus.Fatalf("Error running backing image fetch command: %v.", err)
			}
		},
	}
}

func fetch(c *cli.Context) error {
	url := c.GlobalString("url")
	bimClient := client.NewBackingImageManagerClient(url)
	bi, err := bimClient.Fetch(c.String("name"), c.String("uuid"), c.String("source-file-name"), c.String("checksum"), c.Int64("size"))
	if err != nil {
		return err
	}
	return util.PrintJSON(bi)
}
