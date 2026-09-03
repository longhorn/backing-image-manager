package util

import (
	"testing"

	commonnet "github.com/longhorn/go-common-libs/net"
)

func TestGetSyncServiceAddressWithPodIPHostPortFormatting(t *testing.T) {
	testCases := []struct {
		name          string
		family        commonnet.IPFamily
		podIP         string
		address       string
		expected      string
		expectedCalls int
	}{
		{
			name:          "unspecified IPv4 host-port",
			family:        commonnet.IPFamilyUnspecified,
			podIP:         "192.0.2.10",
			address:       "198.51.100.20:9500",
			expected:      "192.0.2.10:9500",
			expectedCalls: 1,
		},
		{
			name:          "unspecified IPv6 host-port",
			family:        commonnet.IPFamilyUnspecified,
			podIP:         "2001:db8::10",
			address:       "198.51.100.20:9500",
			expected:      "[2001:db8::10]:9500",
			expectedCalls: 1,
		},
		{
			name:          "explicit IPv4 host-port",
			family:        commonnet.IPFamilyIPv4,
			podIP:         "192.0.2.11",
			address:       "198.51.100.20:9500",
			expected:      "192.0.2.11:9500",
			expectedCalls: 1,
		},
		{
			name:          "explicit IPv6 host-port",
			family:        commonnet.IPFamilyIPv6,
			podIP:         "2001:db8::11",
			address:       "198.51.100.20:9500",
			expected:      "[2001:db8::11]:9500",
			expectedCalls: 1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			resolverCalls := 0
			resolvePodIP := func(family commonnet.IPFamily) (string, error) {
				resolverCalls++
				if family != testCase.family {
					t.Fatalf("resolvePodIP() family = %q, want %q", family, testCase.family)
				}
				return testCase.podIP, nil
			}

			address, err := GetSyncServiceAddressWithPodIP(testCase.address, testCase.family, resolvePodIP)
			if err != nil {
				t.Fatalf("GetSyncServiceAddressWithPodIP() error = %v", err)
			}
			if address != testCase.expected {
				t.Fatalf("GetSyncServiceAddressWithPodIP() = %q, want %q", address, testCase.expected)
			}
			if resolverCalls != testCase.expectedCalls {
				t.Fatalf("resolvePodIP() calls = %d, want %d", resolverCalls, testCase.expectedCalls)
			}
		})
	}
}

func TestGetSyncServiceAddressWithPodIPRejectsInvalidAddress(t *testing.T) {
	testCases := []struct {
		name   string
		family commonnet.IPFamily
		input  string
	}{
		{
			name:   "IPv4 family",
			family: commonnet.IPFamilyIPv4,
			input:  "198.51.100.20",
		},
		{
			name:   "IPv6 family",
			family: commonnet.IPFamilyIPv6,
			input:  "[2001:db8::20]",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			address, err := GetSyncServiceAddressWithPodIP(testCase.input, testCase.family, func(commonnet.IPFamily) (string, error) {
				t.Fatal("resolver called for invalid address")
				return "", nil
			})
			if err == nil {
				t.Fatalf("GetSyncServiceAddressWithPodIP() = %q, want an error", address)
			}
			if address != "" {
				t.Fatalf("GetSyncServiceAddressWithPodIP() address = %q, want empty", address)
			}
		})
	}
}
