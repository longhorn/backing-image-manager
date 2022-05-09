package util

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	HTTPClientErrorPrefixTemplate = "resp.StatusCode(%d) != http.StatusOK(200)"
)

func GetHTTPClientErrorPrefix(stateCode int) string {
	return fmt.Sprintf(HTTPClientErrorPrefixTemplate, stateCode)
}

func IsHTTPClientErrorNotFound(inputErr error) bool {
	return inputErr != nil && strings.Contains(inputErr.Error(), GetHTTPClientErrorPrefix(http.StatusNotFound))
}

func DetectHTTPServerAvailability(url string, waitIntervalInSecond int, shouldAvailable bool) bool {
	cli := http.Client{
		Timeout: time.Second,
	}

	endTime := time.Now().Add(time.Duration(waitIntervalInSecond) * time.Second)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C

		_, err := cli.Get(url)
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
