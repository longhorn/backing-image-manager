package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

func BackingImageCmd() cli.Command {
	return cli.Command{
		Name: "backing-image",
		Subcommands: []cli.Command{
			PullCmd(),
			SyncCmd(),
			SendCmd(),
			DeleteCmd(),
			GetCmd(),
			ListCmd(),
			OwnershipTransferStartCmd(),
			OwnershipTransferConfirmCmd(),
		},
	}
}

func PullCmd() cli.Command {
	return cli.Command{
		Name: "pull",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "download-url",
			},
			cli.StringFlag{
				Name: "uuid",
			},
		},
		Action: func(c *cli.Context) {
			if err := pull(c); err != nil {
				logrus.Fatalf("Error running backing image pull command: %v.", err)
			}
		},
	}
}

func pull(c *cli.Context) error {
	url := c.GlobalString("url")
	bimClient := client.NewBackingImageManagerClient(url)
	bi, err := bimClient.Pull(c.String("name"), c.String("download-url"), c.String("uuid"))
	if err != nil {
		return err
	}
	return util.PrintJSON(bi)
}

func SyncCmd() cli.Command {
	return cli.Command{
		Name: "sync",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "download-url",
			},
			cli.StringFlag{
				Name: "uuid",
			},
			cli.StringFlag{
				Name: "from-host",
			},
			cli.StringFlag{
				Name: "to-host",
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
	bi, err := bimClient.Sync(c.String("name"), c.String("download-url"), c.String("uuid"), c.String("from-host"), c.String("to-host"))
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

func OwnershipTransferStartCmd() cli.Command {
	return cli.Command{
		Name:    "ownership-transfer-start",
		Aliases: []string{"ots"},
		Action: func(c *cli.Context) {
			if err := ownershipTransferStart(c); err != nil {
				logrus.Fatalf("Error running backing image ownership transfer start command: %v.", err)
			}
		},
		Usage: "Ask an old manager to prepare for transferring the ownership pf all downloaded and not-sending backing images. This will return the prepared transferring backing images",
	}
}

func ownershipTransferStart(c *cli.Context) error {
	url := c.GlobalString("url")
	bimClient := client.NewBackingImageManagerClient(url)
	biMap, err := bimClient.OwnershipTransferStart()
	if err != nil {
		return err
	}
	return util.PrintJSON(biMap)
}

func OwnershipTransferConfirmCmd() cli.Command {
	return cli.Command{
		Name:    "ownership-transfer-confirm",
		Aliases: []string{"otc"},
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name: "backing-images",
			},
			cli.StringSliceFlag{
				Name: "download-urls",
			},
			cli.StringSliceFlag{
				Name: "uuids",
			},
		},
		Usage: "If '--backing-image' is empty, this command means asking an old manager to give up the ownership of transferring backing images. " +
			"If '--backing-image' is set, this command means asking a new manager to take over the ownership of transferring backing images from an old manager.",
		Action: func(c *cli.Context) {
			if err := ownershipTransferConfirm(c); err != nil {
				logrus.Fatalf("Error running backing image ownership transfer confirm command: %v.", err)
			}
		},
	}
}

func ownershipTransferConfirm(c *cli.Context) error {
	url := c.GlobalString("url")
	biNames := c.StringSlice("backing-images")
	downloadURLs := c.StringSlice("download-urls")
	uuids := c.StringSlice("uuids")
	if len(biNames) != len(downloadURLs) {
		return fmt.Errorf("the length of download URLs doesn't match that of backing images")
	}
	if len(biNames) != len(uuids) {
		return fmt.Errorf("the length of UUIDs doesn't match that of backing images")
	}
	bimClient := client.NewBackingImageManagerClient(url)
	readyBackingImages := map[string]*api.BackingImage{}
	for index, name := range biNames {
		readyBackingImages[name] = &api.BackingImage{
			Name: name,
			URL:  downloadURLs[index],
			UUID: uuids[index],
		}
	}
	return bimClient.OwnershipTransferConfirm(readyBackingImages)
}
