package log

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/indigowar/dmq/internal/core/record"
)

type writeRequest struct {
	Filename string        `json:"filename"`
	Record   record.Record `json:"record"`
}

type writeResponse struct {
	PhysicalPosition int64 `json:"physical_position"`
}

type writeInNewFileRequest struct {
	Dir    string        `json:"dir"`
	Ext    string        `json:"ext"`
	Record record.Record `json:"record"`
}

type writeInNewFileResponse struct {
	File             int   `json:"file"`
	PhysicalPosition int64 `json:"physical_position"`
}

func write(ctx context.Context, request writeRequest) (writeResponse, error) {
	data, err := encodeRecord(request.Record)
	if err != nil {
		return writeResponse{}, err
	}

	file, err := os.OpenFile(request.Filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return writeResponse{}, err
	}

	defer file.Close()

	pos, err := writeInFile(file, data)
	if err != nil {
		return writeResponse{}, err
	}

	return writeResponse{PhysicalPosition: pos}, nil
}

func writeNew(ctx context.Context, request writeInNewFileRequest) (writeInNewFileResponse, error) {
	files, err := os.ReadDir(request.Dir)
	if err != nil {
		return writeInNewFileResponse{}, err
	}

	biggest := 0
	for _, file := range files {
		var (
			number = 0
			ext    = ""
		)

		n, err := fmt.Sscanf(file.Name(), "%08d.%s", &number, &ext)
		if err != nil {
			return writeInNewFileResponse{}, err
		}
		if n != 2 {
			return writeInNewFileResponse{}, errors.New("failed to scan the file name")
		}

		if ext != request.Ext {
			continue
		}

		biggest = max(biggest, number)
	}

	res, err := write(ctx, writeRequest{
		Filename: fmt.Sprintf("%s/%08d.%s", request.Dir, biggest+1, request.Ext),
		Record:   request.Record,
	})
	if err != nil {
		return writeInNewFileResponse{}, err
	}

	return writeInNewFileResponse{
		File:             biggest + 1,
		PhysicalPosition: res.PhysicalPosition,
	}, err
}

func writeInFile(file *os.File, data []byte) (int64, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	position := stat.Size()

	if _, err := file.Write(data); err != nil {
		return 0, err
	}

	return position, nil
}

func encodeRecord(r record.Record) ([]byte, error) {
	record, err := recordToBinary(r)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.LittleEndian, int64(len(record))); err != nil {
		return nil, err
	}

	buf.Write(record)

	return buf.Bytes(), nil
}
