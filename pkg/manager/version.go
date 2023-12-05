package manager

import (
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/backing-image-manager/pkg/meta"
	"github.com/longhorn/backing-image-manager/pkg/rpc"
)

func (pm *Manager) VersionGet(ctx context.Context, empty *emptypb.Empty) (*rpc.VersionResponse, error) {
	v := meta.GetVersion()
	return &rpc.VersionResponse{
		Version:   v.Version,
		GitCommit: v.GitCommit,
		BuildDate: v.BuildDate,

		BackingImageManagerApiVersion:    int64(v.BackingImageManagerAPIVersion),
		BackingImageManagerApiMinVersion: int64(v.BackingImageManagerAPIMinVersion),
	}, nil
}
