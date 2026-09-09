package datasource

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/sparse-tools/sparse"

	commonnet "github.com/longhorn/go-common-libs/net"
	enginerpc "github.com/longhorn/types/pkg/generated/enginerpc"

	"github.com/longhorn/backing-image-manager/pkg/client"
	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"

	filesync "github.com/longhorn/backing-image-manager/pkg/sync"
)

type volumeExportSender struct {
	enginerpc.UnimplementedSyncAgentServiceServer
	filePath string
}

func (s *volumeExportSender) VolumeExport(_ context.Context, req *enginerpc.VolumeExportRequest) (*emptypb.Empty, error) {
	address := net.JoinHostPort(req.Host, strconv.Itoa(int(req.Port)))
	if err := sparse.SyncFile(s.filePath, address, int(req.FileSyncHttpClientTimeout), false, false); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func TestExportFromVolumeUsesConfiguredIPFamily(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	syncServer := httptest.NewUnstartedServer(nil)
	syncService, err := filesync.InitService(ctx, syncServer.Listener.Addr().String(), &filesync.HTTPHandler{})
	if err != nil {
		syncServer.Close()
		t.Fatal(err)
	}
	syncServer.Config.Handler = filesync.NewRouter(syncService)
	syncServer.Start()
	t.Cleanup(syncServer.Close)

	filePath := filepath.Join(t.TempDir(), "export-from-volume")
	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := os.WriteFile(filePath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	checksum, err := util.GetFileChecksum(filePath)
	if err != nil {
		t.Fatal(err)
	}

	replicaListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	replicaPort := replicaListener.Addr().(*net.TCPAddr).Port
	replicaServer := grpc.NewServer()
	enginerpc.RegisterSyncAgentServiceServer(replicaServer, &volumeExportSender{filePath: filePath})
	go func() {
		_ = replicaServer.Serve(replicaListener)
	}()
	defer replicaServer.Stop()

	destinationPath := filepath.Join(t.TempDir(), "received")
	service := &Service{
		ctx:              ctx,
		ipFamily:         commonnet.IPFamilyIPv6,
		filePath:         destinationPath,
		uuid:             "export-from-volume-uuid",
		diskUUID:         "export-from-volume-disk",
		expectedChecksum: checksum,
		log:              logrus.New(),
		resolvePodIP: func(family commonnet.IPFamily) (string, error) {
			if family == commonnet.IPFamilyIPv6 {
				return "::1", nil
			}
			return "", errors.New("IPv6 family is required")
		},
		syncClient: client.SyncClient{Remote: syncServer.Listener.Addr().String()},
	}
	parameters := map[string]string{
		types.DataSourceTypeExportFromVolumeParameterSnapshotName:              "snapshot",
		types.DataSourceTypeExportFromVolumeParameterSenderAddress:             net.JoinHostPort("127.0.0.1", strconv.Itoa(replicaPort-2)),
		types.DataSourceTypeExportFromVolumeParameterVolumeSize:                strconv.Itoa(len(data)),
		types.DataSourceTypeExportFromVolumeParameterFileSyncHTTPClientTimeout: "60",
		types.DataSourceTypeParameterDataEngine:                                types.DataEnginev1,
	}

	if err := service.exportFromVolume(parameters); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		info, err := service.syncClient.Get(destinationPath)
		if err == nil && info.State == string(types.StateReady) {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("timed out waiting for IPv6 volume export")
	}
	received, err := os.ReadFile(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, data) {
		t.Fatalf("exported content = %q, want %q", received, data)
	}
}

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
