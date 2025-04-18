package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

const (
	HTTPClientTimeout = 10 * time.Second
)

type SyncClient struct {
	Remote string
}

func (client *SyncClient) Get(filePath string) (*api.FileInfo, error) {
	httpClient := &http.Client{Timeout: HTTPClientTimeout}

	requestURL := fmt.Sprintf("http://%s/v1/files/%s", client.Remote, url.QueryEscape(filePath))
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%s, failed to read the response body: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	result := &api.FileInfo{}

	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bodyContent, result); err != nil {
		return nil, err
	}

	return result, nil
}

func (client *SyncClient) List() (map[string]*api.FileInfo, error) {
	httpClient := &http.Client{Timeout: HTTPClientTimeout}

	requestURL := fmt.Sprintf("http://%s/v1/files", client.Remote)
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%s, failed to read the response body: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	result := map[string]*api.FileInfo{}

	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bodyContent, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (client *SyncClient) Delete(filePath string) error {
	httpClient := &http.Client{Timeout: HTTPClientTimeout}

	requestURL := fmt.Sprintf("http://%s/v1/files/%s", client.Remote, url.QueryEscape(filePath))
	req, err := http.NewRequest("DELETE", requestURL, nil)
	if err != nil {
		return err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("%s or http.StatusNotFound(%d), response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), http.StatusNotFound, string(bodyContent))
	}

	return nil
}

func (client *SyncClient) Forget(filePath string) error {
	httpClient := &http.Client{Timeout: HTTPClientTimeout}

	requestURL := fmt.Sprintf("http://%s/v1/files/%s", client.Remote, url.QueryEscape(filePath))
	req, err := http.NewRequest("POST", requestURL, nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("action", "forget")
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("forget failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("%s or http.StatusNotFound(%d), response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), http.StatusNotFound, string(bodyContent))
	}

	return nil
}

func (client *SyncClient) Fetch(srcFilePath, dstFilePath, uuid, diskUUID, expectedChecksum string, size int64) error {
	httpClient := &http.Client{Timeout: 0}

	requestURL := fmt.Sprintf("http://%s/v1/files", client.Remote)
	req, err := http.NewRequest("POST", requestURL, nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("action", "fetch")
	q.Add("src-file-path", srcFilePath)
	q.Add("dst-file-path", dstFilePath)
	q.Add("uuid", uuid)
	q.Add("disk-uuid", diskUUID)
	q.Add("expected-checksum", expectedChecksum)
	q.Add("size", strconv.FormatInt(size, 10))
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	return nil
}

func (client *SyncClient) DownloadFromURL(downloadURL, filePath, uuid, diskUUID, expectedChecksum, dataEngine string) error {
	httpClient := &http.Client{Timeout: 0}

	requestURL := fmt.Sprintf("http://%s/v1/files", client.Remote)
	req, err := http.NewRequest("POST", requestURL, nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("action", "downloadFromURL")
	q.Add("url", downloadURL)
	q.Add("file-path", filePath)
	q.Add("uuid", uuid)
	q.Add("disk-uuid", diskUUID)
	q.Add("expected-checksum", expectedChecksum)
	q.Add("data-engine", dataEngine)
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("download from URL failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	return nil
}

func (client *SyncClient) CloneFromBackingImage(sourceBackingImage, sourceBackingImageUUID, encryption, filePath, uuid, diskUUID, expectedChecksum string, credential map[string]string, dataEngine string) error {
	httpClient := &http.Client{Timeout: 0}
	encodedCredential, err := json.Marshal(credential)
	if err != nil {
		return err
	}

	requestURL := fmt.Sprintf("http://%s/v1/files", client.Remote)
	req, err := http.NewRequest("POST", requestURL, bytes.NewReader(encodedCredential))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	q := req.URL.Query()
	q.Add("action", "cloneFromBackingImage")
	q.Add("backing-image", sourceBackingImage)
	q.Add("backing-image-uuid", sourceBackingImageUUID)
	q.Add("encryption", encryption)
	q.Add("file-path", filePath)
	q.Add("uuid", uuid)
	q.Add("disk-uuid", diskUUID)
	q.Add("expected-checksum", expectedChecksum)
	q.Add("data-engine", dataEngine)

	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "clone from backing image failed")
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	return nil
}

func (client *SyncClient) RestoreFromBackupURL(backupURL, concurrentLimit, filePath, uuid, diskUUID, expectedChecksum string, credential map[string]string, dataEngine string) error {
	httpClient := &http.Client{Timeout: 0}
	encodedCredential, err := json.Marshal(credential)
	if err != nil {
		return err
	}

	requestURL := fmt.Sprintf("http://%s/v1/files", client.Remote)
	req, err := http.NewRequest("POST", requestURL, bytes.NewReader(encodedCredential))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	q := req.URL.Query()
	q.Add("action", "restoreFromBackupURL")
	q.Add("backup-url", backupURL)
	q.Add("file-path", filePath)
	q.Add("uuid", uuid)
	q.Add("disk-uuid", diskUUID)
	q.Add("expected-checksum", expectedChecksum)
	q.Add("concurrent-limit", concurrentLimit)
	q.Add("data-engine", dataEngine)

	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("download from URL failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	return nil
}

func (client *SyncClient) Upload(src, dst, uuid, diskUUID, expectedChecksum string) error {
	httpClient := &http.Client{Timeout: 0}

	stat, err := os.Stat(src)
	if err != nil {
		return err
	}

	r, w := io.Pipe()
	m := multipart.NewWriter(w)
	go func() {
		defer func() {
			if errClose := w.Close(); errClose != nil {
				logrus.WithError(errClose).Error("Failed to close writer")
			}
		}()
		defer func() {
			if errClose := m.Close(); errClose != nil {
				logrus.WithError(errClose).Error("Failed to close multipart writer")
			}
		}()
		part, err := m.CreateFormFile("chunk", "blob")
		if err != nil {
			return
		}
		file, err := os.Open(src)
		if err != nil {
			return
		}
		defer func() {
			if errClose := file.Close(); errClose != nil {
				logrus.WithError(errClose).Error("Failed to close file")
			}
		}()
		if _, err = io.Copy(part, file); err != nil {
			return
		}
	}()

	requestURL := fmt.Sprintf("http://%s/v1/files", client.Remote)

	req, err := http.NewRequest("POST", requestURL, r)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("action", "upload")
	q.Add("file-path", dst)
	q.Add("uuid", uuid)
	q.Add("disk-uuid", diskUUID)
	q.Add("expected-checksum", expectedChecksum)
	q.Add("size", strconv.Itoa(int(stat.Size())))
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", m.FormDataContentType())

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("upload failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	return nil
}

func (client *SyncClient) Receive(filePath, uuid, diskUUID, expectedChecksum, fileType string, receiverPort int, size int64, dataEngine string) error {
	httpClient := &http.Client{Timeout: 0}

	requestURL := fmt.Sprintf("http://%s/v1/files", client.Remote)
	req, err := http.NewRequest("POST", requestURL, nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("action", "receiveFromPeer")
	q.Add("file-path", filePath)
	q.Add("uuid", uuid)
	q.Add("disk-uuid", diskUUID)
	q.Add("expected-checksum", expectedChecksum)
	q.Add("file-type", fileType)
	q.Add("port", strconv.Itoa(receiverPort))
	q.Add("size", strconv.FormatInt(size, 10))
	q.Add("data-engine", dataEngine)
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("receive from peer failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	return nil
}

func (client *SyncClient) Send(filePath, toAddress string) error {
	httpClient := &http.Client{Timeout: 0}

	requestURL := fmt.Sprintf("http://%s/v1/files/%s", client.Remote, url.QueryEscape(filePath))
	req, err := http.NewRequest("POST", requestURL, nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("action", "sendToPeer")
	q.Add("to-address", toAddress)
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send to peer failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	bodyContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "%v, failed to read the response body", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s, response body content: %v", util.GetHTTPClientErrorPrefix(resp.StatusCode), string(bodyContent))
	}

	return nil
}

func (client *SyncClient) DownloadToDst(srcFilePath, dstFilePath string) error {
	if _, err := os.Stat(dstFilePath); err == nil || !os.IsNotExist(err) {
		if err := os.RemoveAll(dstFilePath); err != nil {
			return errors.Wrapf(err, "failed to clean up the dst file path before download")
		}
	}
	dst, err := os.Create(dstFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to create the dst file before download")
	}
	defer func() {
		if errClose := dst.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close dst file")
		}
	}()

	httpClient := &http.Client{Timeout: 0}

	requestURL := fmt.Sprintf("http://%s/v1/files/%s/download", client.Remote, url.QueryEscape(srcFilePath))
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("download to dst failed, err: %s", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close response body")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s, skip reading the response body content", util.GetHTTPClientErrorPrefix(resp.StatusCode))
	}

	copied, err := io.Copy(dst, resp.Body)
	if err != nil {
		return err
	}
	if err := dst.Truncate(copied); err != nil {
		return errors.Wrapf(err, "failed to truncate the file after download")
	}
	return nil
}
