package uploadserver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

const (
	ChunkFilePrefix = "backing-chunk-"

	DefaultSectorSize = 512
)

func getChunkFilePath(chunkIndex int64, directory, checksum string) string {
	return filepath.Join(directory, fmt.Sprintf("%s%d-%s", ChunkFilePrefix, chunkIndex, checksum))
}

func listChunksWithIndex(index int64, directory string) ([]string, error) {
	// list all chunks if index is a negative number
	if index < 0 {
		return filepath.Glob(filepath.Join(directory, fmt.Sprintf("%s*", ChunkFilePrefix)))
	}
	return filepath.Glob(filepath.Join(directory, fmt.Sprintf("%s%d-*", ChunkFilePrefix, index)))
}

func getIndexFromFilePath(filePath string) (int64, error) {
	fileName := filepath.Base(filePath)
	if !strings.HasPrefix(fileName, ChunkFilePrefix) {
		return -1, fmt.Errorf("invalid chunk path %v: no prefix %v", filePath, ChunkFilePrefix)
	}
	nameSlice := strings.Split(fileName, "-")
	if len(nameSlice) < 4 {
		return -1, fmt.Errorf("invalid chunk path %v: invalid format", filePath)
	}
	index, err := strconv.ParseInt(nameSlice[2], 10, 64)
	if err != nil {
		return -1, fmt.Errorf("invalid chunk path %v, cannot parse index: %v", filePath, err)
	}
	return index, nil
}

func (server *UploadServer) start(writer http.ResponseWriter, request *http.Request) {
	if err := server.doStart(request); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *UploadServer) doStart(request *http.Request) error {
	queryParams := request.URL.Query()

	size, err := strconv.ParseInt(queryParams.Get("size"), 10, 64)
	if err != nil {
		return err
	}

	if size%DefaultSectorSize != 0 {
		return fmt.Errorf("the uploaded file size %d should be a multiple of %d bytes since Longhorn uses directIO by default", size, DefaultSectorSize)
	}

	server.sizeUpdater.SetUploadSize(size)

	// Create the tmp file that indicates the upload starts
	tmpFilePath := filepath.Join(server.directory, types.BackingImageTmpFileName)
	if err := os.Remove(tmpFilePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(tmpFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	server.log.Debugf("Create the tmp file %v that indicates the upload start", tmpFilePath)

	return nil
}

func (server *UploadServer) prepareChunk(writer http.ResponseWriter, request *http.Request) {
	exists, err := server.doChunkPrepare(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	outgoingJSON, err := json.Marshal(map[string]bool{"exists": exists})
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(outgoingJSON)
}

func (server *UploadServer) doChunkPrepare(request *http.Request) (exists bool, err error) {
	queryParams := request.URL.Query()

	checksum := queryParams.Get("checksum")
	if len(checksum) != util.PreservedChecksumLength {
		return false, fmt.Errorf("invalid checksum %v, the length is not truncated to %v", checksum, util.PreservedChecksumLength)
	}

	size, err := strconv.ParseInt(queryParams.Get("size"), 10, 64)
	if err != nil {
		return false, err
	}

	index, err := strconv.ParseInt(queryParams.Get("index"), 10, 64)
	if err != nil {
		return false, err
	}

	// Create the tmp file that indicates the upload start when receiving
	// the 1st chunk preparation call.
	tmpFilePath := filepath.Join(server.directory, types.BackingImageTmpFileName)
	if _, err := os.Stat(tmpFilePath); err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		server.log.Debugf("Create the tmp file %v that indicates the upload start", tmpFilePath)
		tmpF, err := os.Create(tmpFilePath)
		if err != nil {
			return false, err
		}
		if err := tmpF.Close(); err != nil {
			return false, err
		}
	}

	// Check if the chunk already exists & is valid.
	chunkPath := getChunkFilePath(index, server.directory, checksum)
	chunkList, err := listChunksWithIndex(index, server.directory)
	if err != nil {
		return false, err
	}
	for _, c := range chunkList {
		cleanupRequired := false
		// Remove the files containing correct index but with different checksum.
		if c != chunkPath {
			server.log.Infof("found chunk file %v containing correct index but with different checksum, will remove it first", c)
			cleanupRequired = true
		} else {
			// Check if the chunk with the matching checksum is valid or not.
			if fileData, err := ioutil.ReadFile(c); err != nil {
				server.log.WithError(err).Errorf("failed to read chunk %v, will remove it first", c)
				cleanupRequired = true
			} else {
				fileChecksum := util.GetChecksum(fileData)
				if fileChecksum != checksum {
					server.log.WithError(err).Errorf("the data in chunk %v doesn't match checksum %v, will remove it first", c, checksum)
					cleanupRequired = true
				} else {
					exists = true
				}
			}
		}
		if cleanupRequired {
			if err := os.Remove(c); err != nil && !os.IsNotExist(err) {
				return false, err
			}
		}
	}

	// Create the file if not exists.
	if !exists {
		f, err := os.OpenFile(chunkPath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return false, err
		}
		if err := f.Truncate(size); err != nil {
			return false, err
		}
		if err := f.Close(); err != nil {
			return false, err
		}
	}

	return exists, nil
}

func (server *UploadServer) uploadChunk(writer http.ResponseWriter, request *http.Request) {
	err := server.doChunkUpload(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *UploadServer) doChunkUpload(request *http.Request) error {
	// 25Mi at max. The actual data size will be 20Mi at max.
	err := request.ParseMultipartForm(25 * 1024 * 1024)
	if err != nil {
		return err
	}
	if request.MultipartForm == nil {
		return fmt.Errorf("nil multipart form")
	}

	indexFormValues := request.MultipartForm.Value["index"]
	if indexFormValues == nil || len(indexFormValues) != 1 {
		return fmt.Errorf("invalid value in form 'index', should be a single integer: %v", request.MultipartForm.Value["index"])
	}
	index, err := strconv.ParseInt(indexFormValues[0], 10, 64)
	if err != nil {
		return err
	}

	checksumFormValues := request.MultipartForm.Value["checksum"]
	if checksumFormValues == nil || len(checksumFormValues) != 1 {
		return fmt.Errorf("invalid value in form 'checksum', should be a single string: %v", request.MultipartForm.Value["checksum"])
	}
	checksum := checksumFormValues[0]

	formFile, _, err := request.FormFile("chunk")
	if err != nil {
		return fmt.Errorf("invalid file in form 'chunk': %v", err)
	}
	data, err := ioutil.ReadAll(formFile)
	if err != nil {
		return err
	}
	calculatedChecksum := util.GetChecksum(data)
	if checksum != calculatedChecksum {
		return fmt.Errorf("checksum %v provided by the client is not the same as the calculated checksum %v", checksum, calculatedChecksum)
	}

	chunkPath := getChunkFilePath(index, server.directory, checksum)
	stat, err := os.Stat(chunkPath)
	if err != nil {
		return fmt.Errorf("failed to stat chunk %v before uploading: %v", chunkPath, err)
	}
	if stat.Size() != int64(len(data)) {
		return fmt.Errorf("the prepared chunk %v size %v doesn't match the data size %v", chunkPath, stat.Size(), int64(len(data)))
	}

	f, err := os.OpenFile(chunkPath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	defer f.Sync()

	if _, err := f.Write(data); err != nil {
		return err
	}

	server.Lock()
	server.uploadedSize += stat.Size()
	server.Unlock()
	server.progressUpdater.UpdateSyncFileProgress(stat.Size())

	server.log.Debugf("Succeeded to upload chunk %v, size %v", chunkPath, stat.Size())

	return nil
}

func (server *UploadServer) coalesceChunk(writer http.ResponseWriter, request *http.Request) {
	err := server.doChunkCoalesce(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *UploadServer) doChunkCoalesce(request *http.Request) error {
	queryParams := request.URL.Query()

	server.Lock()
	uploadedSize := server.uploadedSize
	server.Unlock()

	size, err := strconv.ParseInt(queryParams.Get("size"), 10, 64)
	if err != nil {
		return err
	}
	if size != uploadedSize {
		return fmt.Errorf("client uploaded size %v doesn't match the server received size %v", size, uploadedSize)
	}
	count, err := strconv.ParseInt(queryParams.Get("count"), 10, 64)
	if err != nil {
		return err
	}

	chunkList, err := listChunksWithIndex(-1, server.directory)
	if err != nil {
		return err
	}
	sort.Sort(sortableChunks(chunkList))

	tmpFilePath := filepath.Join(server.directory, types.BackingImageTmpFileName)
	f, err := os.OpenFile(tmpFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	defer f.Sync()
	if err := f.Truncate(size); err != nil {
		return err
	}

	coalescedChunkIndexMap := map[int64]struct{}{}
	for _, c := range chunkList {
		index, err := getIndexFromFilePath(c)
		if err != nil {
			server.log.WithError(err).Warn("Found invalid chunk file during coalescing, will directly remove it")
			if err := os.Remove(c); err != nil {
				server.log.WithError(err).Errorf("failed to remove %v during coalescing", c)
			}
			continue
		}
		if index >= count {
			server.log.Infof("Found extra chunk file %v that may be left by previous upload, will directly remove it", c)
			if err := os.Remove(c); err != nil {
				server.log.WithError(err).Errorf("failed to remove %v during coalescing", c)
			}
			continue
		}
		coalescedChunkIndexMap[index] = struct{}{}
		data, err := ioutil.ReadFile(c)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			return err
		}
		if err := os.Remove(c); err != nil {
			server.log.WithError(err).Errorf("failed to remove %v after coalescing", c)
		}
	}
	if len(coalescedChunkIndexMap) != int(count) {
		return fmt.Errorf("coalesced chunk number %v doesn't match the uploaded chunk number %v", len(coalescedChunkIndexMap), count)
	}

	return nil
}

func (server *UploadServer) close(writer http.ResponseWriter, request *http.Request) {
	if f, ok := writer.(http.Flusher); ok {
		f.Flush()
	}
	server.cancelFunc()
}

type sortableChunks []string

func (c sortableChunks) Len() int {
	return len(c)
}

func (c sortableChunks) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c sortableChunks) Less(i, j int) bool {
	// Invalid entry is considered as the smaller one.
	index1, err := getIndexFromFilePath(filepath.Base(c[i]))
	if err != nil {
		return false
	}
	index2, err := getIndexFromFilePath(filepath.Base(c[j]))
	if err != nil {
		return true
	}
	return index1 < index2
}
