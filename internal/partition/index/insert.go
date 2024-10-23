package index

import (
	"context"
	"encoding/binary"
	"os"
)

type InsertReq struct {
	Filename string `json:"filename"`
	Data     Pair   `json:"data"`
}

type InsertRes = struct{}

func Write(ctx context.Context, request InsertReq) (InsertRes, error) {
	buffer := make([]byte, pairSize)
	if _, err := binary.Encode(buffer, binary.NativeEndian, request.Data); err != nil {
		return InsertRes{}, err
	}

	file, err := os.OpenFile(request.Filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return InsertRes{}, err
	}
	defer file.Close()

	if _, err := file.Write(buffer); err != nil {
		return InsertRes{}, err
	}

	return InsertRes{}, nil
}
