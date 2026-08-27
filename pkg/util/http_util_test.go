package util

import (
	"errors"
	"testing"
)

func TestGetSyncServiceAddressWithPodIPHostPortFormatting(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		podIP    string
		address  string
		expected string
	}{
		{
			name:     "IPv4 host-port",
			podIP:    "192.0.2.10",
			address:  "198.51.100.20:9500",
			expected: "192.0.2.10:9500",
		},
		{
			name:     "IPv6 host-port",
			podIP:    "2001:db8::10",
			address:  "198.51.100.20:9500",
			expected: "[2001:db8::10]:9500",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			resolverCalls := 0
			resolvePodIP := func() (string, error) {
				resolverCalls++
				return testCase.podIP, nil
			}

			address, err := GetSyncServiceAddressWithPodIP(testCase.address, resolvePodIP)
			if err != nil {
				t.Fatalf("GetSyncServiceAddressWithPodIP() error = %v", err)
			}
			if address != testCase.expected {
				t.Fatalf("GetSyncServiceAddressWithPodIP() = %q, want %q", address, testCase.expected)
			}
			if resolverCalls != 1 {
				t.Fatalf("resolvePodIP() calls = %d, want 1", resolverCalls)
			}
		})
	}
}

func TestGetSyncServiceAddressWithPodIPPropagatesResolverError(t *testing.T) {
	expectedErr := errors.New("resolver failed")
	address, err := GetSyncServiceAddressWithPodIP("198.51.100.20:9500", func() (string, error) {
		return "", expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("GetSyncServiceAddressWithPodIP() error = %v, want %v", err, expectedErr)
	}
	if address != "" {
		t.Fatalf("GetSyncServiceAddressWithPodIP() address = %q, want empty", address)
	}
}

func TestGetSyncServiceAddressWithPodIPRejectsInvalidAddress(t *testing.T) {
	address, err := GetSyncServiceAddressWithPodIP("198.51.100.20", func() (string, error) {
		t.Fatal("resolver called for invalid address")
		return "", nil
	})
	if err == nil {
		t.Fatalf("GetSyncServiceAddressWithPodIP() = %q, want an error", address)
	}
	if address != "" {
		t.Fatalf("GetSyncServiceAddressWithPodIP() address = %q, want empty", address)
	}
}
