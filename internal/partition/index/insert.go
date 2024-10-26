package index

import (
	"context"
	"encoding/binary"
	"os"
)

type insertRequest struct {
	Filename string `json:"filename"`
	Data     Pair   `json:"data"`
}

func Insert(ctx context.Context, request insertRequest) (noResponse, error) {
	buffer := make([]byte, pairSize)
	if _, err := binary.Encode(buffer, binary.NativeEndian, request.Data); err != nil {
		return noResponse{}, err
	}

	file, err := os.OpenFile(request.Filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return noResponse{}, err
	}
	defer file.Close()

	if _, err := file.Write(buffer); err != nil {
		return noResponse{}, err
	}

	return noResponse{}, nil
}
