package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
)

type findRequest struct {
	Filename string `json:"filename"`
	Key      int64  `json:"key"`
}

type findResponse struct {
	Value int64 `json:"value"`
}

func find(ctx context.Context, request findRequest) (findResponse, error) {
	file, err := os.OpenFile(request.Filename, os.O_RDONLY, 0644)
	if err != nil {
		return findResponse{}, err
	}
	defer file.Close()

	position := int64(0)
	buffer := make([]byte, pairSize)
	for {
		if _, err := file.ReadAt(buffer, position); err != nil {
			return findResponse{}, err
		}

		var pair Pair
		reader := bytes.NewReader(buffer)
		if err := binary.Read(reader, binary.NativeEndian, &pair); err != nil {
			return findResponse{}, err
		}

		if pair.Key == request.Key {
			return findResponse{Value: pair.Value}, nil
		}
	}
}
