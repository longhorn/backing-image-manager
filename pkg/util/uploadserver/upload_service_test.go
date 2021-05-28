package uploadserver

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backing-image-manager/pkg/util"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {
	_, err := getTestWorkDirectory()
	c.Assert(err, IsNil)
}

func (s *TestSuite) TearDownSuite(c *C) {
	dir, err := getTestWorkDirectory()
	c.Assert(err, IsNil)
	os.RemoveAll(dir)
}

func (s *TestSuite) TestUploadService(c *C) {
	const (
		localhost = "localhost"
		port      = "31001"
	)

	dir, err := getTestWorkDirectory()
	c.Assert(err, IsNil)
	originalFilePath := filepath.Join(dir, "original")
	err = util.GenerateRandomDataFile(originalFilePath, 100)
	c.Assert(err, IsNil)

	stub := &progressUpdateStub{}
	go TestUploadServer(context.Background(), port, dir, stub, stub)
	cli := UploadClient{
		Remote:    localhost + ":" + port,
		Directory: dir,
	}

	f, err := os.OpenFile(originalFilePath, os.O_RDONLY, 0666)
	c.Assert(err, IsNil)
	defer f.Close()

	stat, err := f.Stat()
	c.Assert(err, IsNil)
	fileSize := stat.Size()

	// Wait for server starting
	for retry := 0; retry < 5; retry++ {
		if err = cli.Start(fileSize); err == nil {
			break
		}
	}
	c.Assert(err, IsNil)
	c.Assert(stub.size, Equals, fileSize)

	offset := int64(0)
	count := int64(0)
	for index := 0; offset < fileSize; index++ {
		// Upload 2Mi ~ 4Mi data in each HTTP Post
		chunkSize := rand.Int63n(2*1024*1024) + 2*1024*1024
		if fileSize-offset <= chunkSize {
			chunkSize = fileSize - offset
		}
		data := make([]byte, chunkSize)
		_, err = f.ReadAt(data, offset)
		c.Assert(err, IsNil)
		offset += chunkSize
		chunkPath := filepath.Join(dir, fmt.Sprintf("%s%d-%s", ChunkFilePrefix, index, util.GetChecksum(data)))

		// Create 2 invalid chunk files. Verify that this files won't
		// intervene the whole upload.
		// There are 2 kinds of invalid files:
		//   1. Contains correct index, but with invalid name and data
		//   2. Contains correct index and name, but with invalid data
		invalidData1 := util.RandStringBytes(8)
		invalidChunkPath1 := filepath.Join(dir, fmt.Sprintf("%s%d-%s", ChunkFilePrefix, index, invalidData1))
		f, err := os.OpenFile(invalidChunkPath1, os.O_RDWR|os.O_CREATE, 0666)
		c.Assert(err, IsNil)
		_, err = f.Write(invalidData1)
		c.Assert(err, IsNil)
		err = f.Close()
		c.Assert(err, IsNil)
		invalidData2 := data[:len(data)-1]
		invalidChunkPath2 := chunkPath
		f, err = os.OpenFile(invalidChunkPath2, os.O_RDWR|os.O_CREATE, 0666)
		c.Assert(err, IsNil)
		_, err = f.Write(invalidData2)
		c.Assert(err, IsNil)
		err = f.Close()
		c.Assert(err, IsNil)

		exists, err := cli.PrepareChunk(index, data)
		c.Assert(err, IsNil)
		c.Assert(exists, Equals, false)

		// The invalid chunk file should be cleaned up after the check call.
		_, err = os.Stat(invalidChunkPath1)
		c.Assert(os.IsNotExist(err), Equals, true)
		_, err = os.Stat(chunkPath)
		c.Assert(err, IsNil)

		err = cli.UploadChunk(index, data)
		c.Assert(err, IsNil)
		count++
	}

	extraChunkPathMap := map[string]struct{}{}
	for extra := int64(0); extra < 5; extra++ {
		index := extra + count
		extraData := util.RandStringBytes(8)
		extraChunkPath := filepath.Join(dir, fmt.Sprintf("%s%d-%s", ChunkFilePrefix, index, extraData))
		extraChunkPathMap[extraChunkPath] = struct{}{}
		f, err := os.OpenFile(extraChunkPath, os.O_RDWR|os.O_CREATE, 0666)
		c.Assert(err, IsNil)
		_, err = f.Write(extraData)
		c.Assert(err, IsNil)
		err = f.Close()
		c.Assert(err, IsNil)
	}

	err = cli.CoalesceChunk(fileSize, count)
	c.Assert(err, IsNil)

	for p := range extraChunkPathMap {
		_, err := os.Stat(p)
		c.Assert(os.IsNotExist(err), Equals, true)
	}

	cli.Close()

	uploadedFilePath := filepath.Join(dir, types.BackingImageTmpFileName)
	err = exec.Command("diff", originalFilePath, uploadedFilePath).Run()
	c.Assert(err, IsNil)
}

func Benchmark_NewUploadAPI(b *testing.B) {
	const (
		localhost = "localhost"
		port      = "31001"
	)

	dir, err := getTestWorkDirectory()
	if err != nil {
		b.Fatal(err)
	}
	originalFilePath := filepath.Join(dir, "original")
	err = util.GenerateRandomDataFile(originalFilePath, 500)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	stub := &progressUpdateStub{}
	go TestUploadServer(context.Background(), port, dir, stub, stub)
	time.Sleep(time.Second)

	cli := UploadClient{
		Remote:    localhost + ":" + port,
		Directory: dir,
	}
	err = cli.Upload(originalFilePath)
	if err != nil {
		b.Fatal(err)
	}
	cli.Close()

	uploadedFilePath := filepath.Join(dir, types.BackingImageTmpFileName)
	err = exec.Command("diff", originalFilePath, uploadedFilePath).Run()
	if err != nil {
		b.Fatal(err)
	}

	// Cleanup
	err = os.RemoveAll(dir)
	if err != nil {
		b.Fatal(err)
	}
}

func getTestWorkDirectory() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(currentUser.HomeDir, "upload-test-dir")
	if err = os.RemoveAll(dir); err != nil {
		return "", err
	}
	err = os.Mkdir(dir, 0777)
	if err != nil {
		return "", err
	}
	return dir, nil
}
