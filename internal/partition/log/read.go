package log

import (
	"context"
	"encoding/binary"
	"errors"
	"os"

	"github.com/indigowar/dmq/internal/core/record"
)

type ReadReq struct {
	Filename string `json:"filename"`
	Position int64  `json:"position"`
}

type ReadRes = record.Record

func Read(ctx context.Context, request ReadReq) (ReadRes, error) {
	file, err := os.OpenFile(request.Filename, os.O_RDONLY, 0644)
	if err != nil {
		return ReadRes{}, errors.New("failed to open the file")
	}
	defer file.Close()

	headerBuf := make([]byte, 8) // read an int64

	if _, err := file.ReadAt(headerBuf, request.Position); err != nil {
		return ReadRes{}, err
	}

	var header int64
	if _, err := binary.Decode(headerBuf, binary.NativeEndian, &header); err != nil {
		return ReadRes{}, err
	}

	recordBuffer := make([]byte, header)
	if _, err := file.ReadAt(recordBuffer, request.Position+16); err != nil {
		return ReadRes{}, err
	}

	return decodeRecord(recordBuffer)
}
