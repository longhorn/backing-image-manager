package api

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/longhorn/backing-image-manager/pkg/rpc"
)

type BackingImage struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	Directory string `json:"directory"`

	Status BackingImageStatus `json:"status"`
}

type BackingImageStatus struct {
	State         string `json:"state"`
	IsSending     bool   `json:"isSending"`
	ErrorMsg      string `json:"errorMsg"`
	SenderAddress string `json:"senderAddress"`
}

func RPCToBackingImage(obj *rpc.BackingImageResponse) *BackingImage {
	return &BackingImage{
		Name:      obj.Spec.Name,
		URL:       obj.Spec.Url,
		Directory: obj.Spec.Directory,

		Status: BackingImageStatus{
			State:         obj.Status.State,
			IsSending:     obj.Status.IsSending,
			ErrorMsg:      obj.Status.ErrorMsg,
			SenderAddress: obj.Status.SenderAddress,
		},
	}
}

func RPCToBackingImageList(obj *rpc.ListResponse) map[string]*BackingImage {
	ret := map[string]*BackingImage{}
	for name, bi := range obj.BackingImages {
		ret[name] = RPCToBackingImage(bi)
	}
	return ret
}

type BackingImageStream struct {
	conn      *grpc.ClientConn
	ctxCancel context.CancelFunc
	stream    rpc.BackingImageManagerService_WatchClient
}

func NewBackingImageStream(conn *grpc.ClientConn, ctxCancel context.CancelFunc, stream rpc.BackingImageManagerService_WatchClient) *BackingImageStream {
	return &BackingImageStream{
		conn,
		ctxCancel,
		stream,
	}
}

func (s *BackingImageStream) Close() error {
	s.ctxCancel()
	if err := s.conn.Close(); err != nil {
		return errors.Wrapf(err, "error closing backing image watcher gRPC connection")
	}
	return nil
}

func (s *BackingImageStream) Recv() error {
	_, err := s.stream.Recv()
	return err
}
