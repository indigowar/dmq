package log

import (
	"context"
	"encoding/binary"
	"os"

	"github.com/indigowar/dmq/internal/core/record"
)

type WriteReq struct {
	Filename string        `json:"filename"`
	Record   record.Record `json:"record"`
}

type WriteRes struct {
	PhysicalPosition int64 `json:"physical_position"`
}

func Write(ctx context.Context, request WriteReq) (WriteRes, error) {
	data, err := encodeRecord(request.Record)
	if err != nil {
		return WriteRes{}, err
	}

	header := make([]byte, 8)
	if _, err := binary.Encode(header, binary.NativeEndian, len(data)); err != nil {
		return WriteRes{}, err
	}

	file, err := os.OpenFile(request.Filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return WriteRes{}, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return WriteRes{}, err
	}

	position := stat.Size()

	if _, err := file.Write(header); err != nil {
		return WriteRes{}, err
	}

	if _, err := file.Write(data); err != nil {
		return WriteRes{}, err
	}

	return WriteRes{
		PhysicalPosition: position,
	}, nil
}
