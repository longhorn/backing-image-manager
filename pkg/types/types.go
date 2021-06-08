package types

import (
	"time"
)

const (
	BackingImageManagerDirectoryName = "backing-images"
	DiskPathInContainer              = "/data/"

	DefaultManagerPort          = 8000
	DefaultDataSourceServerPort = 8001

	GRPCServiceTimeout     = 3 * time.Minute
	FileValidationInterval = 5 * time.Second
	FileSyncTimeout        = 120

	SendingLimit = 3

	BackingImageTmpFileName = "backing.tmp"
	BackingImageFileName    = "backing"
)

type State string

const (
	StatePending    = State("pending")
	StateStarting   = State("starting")
	StateInProgress = State("in-progress")
	StateReady      = State("ready")
	StateFailed     = State("failed")
)
