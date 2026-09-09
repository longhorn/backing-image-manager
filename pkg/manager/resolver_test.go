package manager

import (
	"context"
	"testing"

	commonnet "github.com/longhorn/go-common-libs/net"
)

func TestNewManagerRejectsNilPodIPResolver(t *testing.T) {
	_, err := NewManager(context.Background(), "", commonnet.IPFamilyUnspecified, "", "", "", nil)
	if err == nil {
		t.Fatal("expected a nil pod IP resolver to be rejected")
	}
}
