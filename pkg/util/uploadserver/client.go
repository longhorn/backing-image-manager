package uploadserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/pkg/util"
)

const (
	TestHTTPTimeout = 10 * time.Second
)

type UploadClient struct {
	Remote    string
	Directory string
}

func (client *UploadClient) Start(size int64) error {
	queries := make(map[string]string)
	queries["size"] = strconv.FormatInt(size, 10)

	resp, err := client.sendHTTPRequest("POST", "start", queries, nil)
	if err != nil {
		return fmt.Errorf("start failed, err: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		httpErr, rErr := ioutil.ReadAll(resp.Body)
		if rErr != nil {
			return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK, err is unknown", resp.StatusCode)
		}
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK, err: %v", resp.StatusCode, string(httpErr))
	}
	resp.Body.Close()

	return nil
}

func (client *UploadClient) PrepareChunk(index int, data []byte) (bool, error) {
	queries := make(map[string]string)
	queries["size"] = strconv.FormatInt(int64(len(data)), 10)
	queries["index"] = strconv.FormatInt(int64(index), 10)
	queries["checksum"] = util.GetChecksum(data)

	resp, err := client.sendHTTPRequest("POST", "prepareChunk", queries, nil)
	if err != nil {
		return false, fmt.Errorf("checkChunk failed, err: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		httpErr, rErr := ioutil.ReadAll(resp.Body)
		if rErr != nil {
			return false, fmt.Errorf("resp.StatusCode(%d) != http.StatusOK, err is unknown", resp.StatusCode)
		}
		return false, fmt.Errorf("resp.StatusCode(%d) != http.StatusOK, err: %v", resp.StatusCode, string(httpErr))
	}
	defer resp.Body.Close()

	result := map[string]bool{}
	bodyContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(bodyContent, &result); err != nil {
		return false, err
	}

	return result["exists"], nil
}

func (client *UploadClient) CoalesceChunk(size, count int64) error {
	queries := make(map[string]string)
	queries["size"] = strconv.FormatInt(size, 10)
	queries["count"] = strconv.FormatInt(count, 10)

	resp, err := client.sendHTTPRequest("POST", "coalesceChunk", queries, nil)
	if err != nil {
		return fmt.Errorf("coalesceChunk failed, err: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		httpErr, rErr := ioutil.ReadAll(resp.Body)
		if rErr != nil {
			return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK, err is unknown", resp.StatusCode)
		}
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK, err: %v", resp.StatusCode, string(httpErr))
	}
	resp.Body.Close()

	return nil
}

func (client *UploadClient) Close() {
	client.sendHTTPRequest("POST", "close", map[string]string{}, nil)
}

func (client *UploadClient) sendHTTPRequest(method string, action string, queries map[string]string, data []byte) (*http.Response, error) {
	httpClient := &http.Client{Timeout: TestHTTPTimeout}

	url := fmt.Sprintf("http://%s/v1-bi-upload/%s", client.Remote, action)

	var req *http.Request
	var err error
	if data != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(data))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/json")

	q := req.URL.Query()
	for k, v := range queries {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	log.Tracef("method: %s, url with query string: %s, data len: %d", method, req.URL.String(), len(data))

	return httpClient.Do(req)
}

func (client *UploadClient) UploadChunk(index int, data []byte) error {
	httpClient := &http.Client{Timeout: TestHTTPTimeout}

	boundary := "----WebKitFormBoundaryLonghornUploadServer" + string(util.RandStringBytes(8))
	bodyData := "--" + boundary + "\n" +
		"Content-Disposition: form-data; name=\"index\";\n\n" +
		strconv.FormatInt(int64(index), 10) + "\n" +
		"--" + boundary + "\n" +
		"Content-Disposition: form-data; name=\"checksum\";\n\n" +
		util.GetChecksum(data) + "\n" +
		"--" + boundary + "\n" +
		"Content-Disposition: form-data; name=\"chunk\"; filename=\"blob\"\n" +
		"Content-Type: application/octet-stream\n\n" +
		string(data) + "\n" +
		"--" + boundary + "--"

	url := fmt.Sprintf("http://%s/v1-bi-upload/%s", client.Remote, "uploadChunk")

	req, err := http.NewRequest("POST", url, strings.NewReader(bodyData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)

	log.Tracef("method: POST, url with query string: %s, data len: %d", req.URL.String(), len(data))

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("uploadChunk failed, err: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		httpErr, rErr := ioutil.ReadAll(resp.Body)
		if rErr != nil {
			return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK, err is unknown", resp.StatusCode)
		}
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK, err: %v", resp.StatusCode, string(httpErr))
	}
	resp.Body.Close()

	return nil
}
