package util

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	commonnet "github.com/longhorn/go-common-libs/net"
)

const HTTPClientErrorPrefixTemplate = "resp.StatusCode(%d) != http.StatusOK(200)"

type PodIPResolver func(commonnet.IPFamily) (string, error)

// NoProxyTransport is a copy of http.DefaultTransport with Proxy disabled.
// Use it for all intra-pod HTTP calls so that requests are never forwarded to
// an external proxy.
// Ref: https://github.com/longhorn/longhorn/issues/12779
var NoProxyTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	return t
}()

func GetHTTPClientErrorPrefix(stateCode int) string {
	return fmt.Sprintf(HTTPClientErrorPrefixTemplate, stateCode)
}

func IsHTTPClientErrorNotFound(inputErr error) bool {
	return inputErr != nil && strings.Contains(inputErr.Error(), GetHTTPClientErrorPrefix(http.StatusNotFound))
}

func DetectHTTPServerAvailability(url string, waitIntervalInSecond int, shouldAvailable bool) bool {
	cli := http.Client{Timeout: time.Second, Transport: NoProxyTransport}

	endTime := time.Now().Add(time.Duration(waitIntervalInSecond) * time.Second)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C

		resp, err := cli.Get(url)
		if resp != nil && resp.Body != nil {
			if err := resp.Body.Close(); err != nil {
				logrus.WithError(err).Error("failed to close the response body during the HTTP server detection")
			}
		}
		if err != nil && !shouldAvailable {
			return true
		}
		if err == nil && shouldAvailable {
			return true
		}
		if !time.Now().Before(endTime) {
			return false
		}
	}
}

func GetIPForPod(family commonnet.IPFamily) (ip string, err error) {
	if family == commonnet.IPFamilyUnspecified {
		return commonnet.GetIPForPodByNetwork()
	}
	return commonnet.GetIPForPodByNetworkAndFamily(family)
}

func GetSyncServiceAddressWithPodIP(address string, family commonnet.IPFamily,
	resolvePodIP PodIPResolver) (string, error) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}

	podIP, err := resolvePodIP(family)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(podIP, port), nil
}
