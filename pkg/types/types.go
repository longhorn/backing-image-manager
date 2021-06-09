package types

import (
	"time"
)

const (
	BackingImageManagerDirectoryName = "backing-images"
	DiskPathInContainer              = "/data/"

	DefaultPort = 8000

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
