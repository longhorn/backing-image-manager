package datasource

import (
	"errors"
	"testing"

	commonnet "github.com/longhorn/go-common-libs/net"

	"github.com/longhorn/backing-image-manager/pkg/types"
)

func TestExportFromVolumePreservesResolverError(t *testing.T) {
	resolverErr := errors.New("no usable IPv6 address")
	service := &Service{
		ipFamily: commonnet.IPFamilyIPv6,
		resolvePodIP: func(commonnet.IPFamily) (string, error) {
			return "", resolverErr
		},
	}
	parameters := map[string]string{
		types.DataSourceTypeExportFromVolumeParameterSnapshotName:              "snapshot",
		types.DataSourceTypeExportFromVolumeParameterSenderAddress:             "127.0.0.1:9502",
		types.DataSourceTypeExportFromVolumeParameterVolumeSize:                "0",
		types.DataSourceTypeExportFromVolumeParameterFileSyncHTTPClientTimeout: "60",
	}

	err := service.exportFromVolume(parameters)
	if !errors.Is(err, resolverErr) {
		t.Fatalf("expected resolver error to be preserved, got %v", err)
	}
}
