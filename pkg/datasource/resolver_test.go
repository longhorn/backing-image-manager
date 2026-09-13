package datasource

import (
	"context"
	"testing"
)

func TestLaunchServiceRejectsNilPodIPResolver(t *testing.T) {
	_, err := LaunchService(context.Background(), nil, "", "", "", "", "", "", nil, nil, nil)
	if err == nil {
		t.Fatal("expected a nil pod IP resolver to be rejected")
	}
}
