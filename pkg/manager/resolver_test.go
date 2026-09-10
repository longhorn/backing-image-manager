package manager

import (
	"context"
	"testing"
)

func TestNewManagerRejectsNilPodIPResolver(t *testing.T) {
	_, err := NewManager(context.Background(), "", "", "", "", nil)
	if err == nil {
		t.Fatal("expected a nil pod IP resolver to be rejected")
	}
}
