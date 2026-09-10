package util

import (
	"testing"
)

func TestGetBackingImageDownloadAddressHostPortFormatting(t *testing.T) {
	testCases := []struct {
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
			address:  "[2001:db8::20]:9500",
			expected: "[2001:db8::10]:9500",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("POD_IP", testCase.podIP)
			address, err := GetBackingImageDownloadAddress(testCase.address)
			if err != nil {
				t.Fatalf("GetBackingImageDownloadAddress() error = %v", err)
			}
			if address != testCase.expected {
				t.Fatalf("GetBackingImageDownloadAddress() = %q, want %q", address, testCase.expected)
			}
		})
	}
}

func TestGetBackingImageDownloadAddressRejectsInvalidAddress(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "missing port",
			input: "198.51.100.20",
		},
		{
			name:  "missing port for IPv6",
			input: "[2001:db8::20]",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			address, err := GetBackingImageDownloadAddress(testCase.input)
			if err == nil {
				t.Fatalf("GetBackingImageDownloadAddress() = %q, want an error", address)
			}
			if address != "" {
				t.Fatalf("GetBackingImageDownloadAddress() address = %q, want empty", address)
			}
		})
	}
}
