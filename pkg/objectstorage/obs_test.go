package objectstorage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path"
	"testing"

	clientutil "d7y.io/dragonfly/v2/client/util"
	testifyassert "github.com/stretchr/testify/assert"
)

const (
	DataDir     = "../../test/testdata"
	DataFile    = "testData"
	DataFileOut = "testData.out"
	BatchSize   = 15 * 1024 * 1024
)

var fileTotalSize int64
var totalPieceCnt int32

type Range struct {
	Start, Length int64
}

type PieceMetadata struct {
	Num    int32  `json:"num,omitempty"`
	Md5    string `json:"md5,omitempty"`
	Offset uint64 `json:"offset,omitempty"`
	Range  Range  `json:"range,omitempty"`
	Cost   uint64 `json:"cost,omitempty"`
}

type ReadPieceRequest struct {
	PieceMetadata
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestReadPiece_pipeToObs(t *testing.T) {
	assert := testifyassert.New(t)

	err := doFileSize()
	assert.Nil(err, "stat data file")
	if err != nil {
		t.Skipf("stat data file")
	}

	dataFile := path.Join(DataDir, DataFile)
	md5Test, err := calcFileMd5(dataFile, nil)
	assert.Nil(err, "calc local file md5")
	if err != nil {
		t.Skipf("calc local file md5")
	}

	pr, pw := io.Pipe()
	var readCloser io.ReadCloser = pr
	go writeToPipe(pw)

	err = pushToOwnBackend(readCloser)
	assert.Nil(err, "push data to starlight")
	if err != nil {
		t.Skipf("push data to starlight")
	}
	readCloser.Close()

	dataFileOut := path.Join(DataDir, DataFileOut)
	fd, err := os.Create(dataFileOut)
	assert.Nil(err, "need pulled file exists")
	if err != nil {
		t.Skipf("need pulled file exists")
	}

	err = downloadFromObs(fd)
	assert.Nil(err, "pull data from starlight")
	if err != nil {
		t.Skipf("pull data from starlight")
	}

	md5TestOut, err := calcFileMd5(dataFileOut, nil)
	assert.Nil(err, "calc remote file md5")
	if err != nil {
		t.Skipf("calc remote file md5")
	}

	assert.Equal(md5Test, md5TestOut)
}

func calcFileMd5(filePath string, rg *clientutil.Range) (string, error) {
	var md5String string
	file, err := os.Open(filePath)
	if err != nil {
		return md5String, err
	}
	defer file.Close()

	var rd io.Reader = file
	if rg != nil {
		rd = io.LimitReader(file, rg.Length)
		_, err = file.Seek(rg.Start, io.SeekStart)
		if err != nil {
			return "", err
		}
	}

	hash := md5.New()
	if _, err := io.Copy(hash, rd); err != nil {
		return md5String, err
	}
	hashInBytes := hash.Sum(nil)[:16]
	md5String = hex.EncodeToString(hashInBytes)
	return md5String, nil
}

func doFileSize() error {
	dataFile := path.Join(DataDir, DataFile)
	file, err := os.Open(dataFile)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	fileTotalSize = stat.Size()
	totalPieceCnt = int32(fileTotalSize/BatchSize + 1)

	return nil
}

func readPiece(req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	dataFile := path.Join(DataDir, DataFile)
	file, err := os.Open(dataFile)
	if err != nil {
		return nil, nil, err
	}

	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		file.Close()
		return nil, nil, err
	}
	// who call ReadPiece, who close the io.ReadCloser
	return io.LimitReader(file, req.Range.Length), file, nil
}

func writeOnePiece(w io.Writer, pieceNum int32) (int64, error) {
	var length int64
	length = BatchSize
	if pieceNum == totalPieceCnt-1 {
		length = fileTotalSize - int64(BatchSize*pieceNum)
	}

	pr, pc, err := readPiece(&ReadPieceRequest{
		PieceMetadata: PieceMetadata{
			Num: pieceNum,
			Range: Range{
				Start:  BatchSize * int64(pieceNum),
				Length: length,
			},
		},
	})
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(w, pr)
	if err != nil {
		pc.Close()
		return n, err
	}

	return n, pc.Close()
}

func writeOrderedPieces(desired, orderedNum int32, pw *io.PipeWriter) (int32, error) {
	for {
		_, err := writeOnePiece(pw, desired)
		if err != nil {

			_ = pw.CloseWithError(err)
			return desired, err
		}

		desired++
		if desired > orderedNum {
			break
		}
	}
	return desired, nil
}

func writeToPipe(pw *io.PipeWriter) {
	var (
		desired int32
	)

	desired = 0
	_, err := writeOrderedPieces(desired, totalPieceCnt-1, pw)
	if err != nil {
		return
	}

	//desired = 63
	//log.Printf("Begin writeRemainingPieces........, From 63 pieces........")
	//writeRemainingPieces(desired, pw)
	pw.Close()
	return
}

func pushToOwnBackend(pr io.ReadCloser) error {
	var (
		obsAk       = "87EBRHPKZANDOK2L3SAO"
		obsSk       = "NoDa1CCmdCnviedZNx68p4WJmiFuNyjuJ7l770qb"
		obsEndpoint = "starlight.cn-central-231.xckpjs.com"
	)

	client, err := newOBS("", obsEndpoint, obsAk, obsSk)
	if err != nil {
		return err
	}

	err = client.PutObject(context.Background(), "urchincache", "demo_testcase.dat", "", pr)
	if err != nil {
		return err
	}

	return nil
}

func downloadFromObs(fd *os.File) error {
	var (
		obsAk       = "87EBRHPKZANDOK2L3SAO"
		obsSk       = "NoDa1CCmdCnviedZNx68p4WJmiFuNyjuJ7l770qb"
		obsEndpoint = "starlight.cn-central-231.xckpjs.com"
	)

	client, err := newOBS("", obsEndpoint, obsAk, obsSk)
	if err != nil {
		return err
	}

	output, err := client.GetOject(context.Background(), "urchincache", "demo_testcase.dat")
	if err != nil {
		return err
	}
	defer output.Close()

	if _, err := io.Copy(fd, output); err != nil {
		log.Printf("io.Copy failed, n:%v, err:%s", output, err.Error())
		return err
	}

	return nil
}
