package log

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"

	"github.com/indigowar/dmq/internal/core/record"
)

type readRequest struct {
	Filename string `json:"filename"`
	Position int64  `json:"position"`
}

type readResponse = record.Record

func read(ctx context.Context, request readRequest) (readResponse, error) {
	file, err := os.OpenFile(request.Filename, os.O_RDONLY, 0644)
	if err != nil {
		return readResponse{}, errors.New("failed to open the file")
	}
	defer file.Close()

	headerBuf := make([]byte, 8) // read an int64

	if _, err := file.ReadAt(headerBuf, request.Position); err != nil {
		return readResponse{}, err
	}

	var header int64
	if err := binary.Read(bytes.NewReader(headerBuf), endian, &header); err != nil {
		return readResponse{}, err
	}

	recordBuffer := make([]byte, header)
	if _, err := file.ReadAt(recordBuffer, request.Position+8); err != nil {
		return readResponse{}, err
	}

	return recordFromBinary(recordBuffer)
}
