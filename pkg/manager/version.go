package manager

import (
	"context"

	rpc "github.com/longhorn/types/pkg/generated/bimrpc"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/backing-image-manager/pkg/meta"
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
