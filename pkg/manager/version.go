package manager

import (
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"

	"github.com/longhorn/backing-image-manager/pkg/meta"
	"github.com/longhorn/backing-image-manager/pkg/rpc"
)

func (pm *Manager) VersionGet(ctx context.Context, empty *empty.Empty) (*rpc.VersionResponse, error) {
	v := meta.GetVersion()
	return &rpc.VersionResponse{
		Version:   v.Version,
		GitCommit: v.GitCommit,
		BuildDate: v.BuildDate,

		BackingImageManagerApiVersion:    int64(v.BackingImageManagerAPIVersion),
		BackingImageManagerApiMinVersion: int64(v.BackingImageManagerAPIMinVersion),
	}, nil
}
