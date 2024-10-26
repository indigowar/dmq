package index

import (
	"context"
	"encoding/binary"
	"io"
	"os"
)

type latestRequest struct {
	Filename string `json:"filename"`
}

func latest(ctx context.Context, request latestRequest) (Pair, error) {
	file, err := os.OpenFile(request.Filename, os.O_RDONLY, 0644)
	if err != nil {
		return Pair{}, err
	}
	defer file.Close()

	if _, err := file.Seek(pairSize, io.SeekEnd); err != nil {
		return Pair{}, err
	}

	buffer := make([]byte, pairSize)
	if _, err := file.Read(buffer); err != nil {
		return Pair{}, err
	}

	var response Pair
	if _, err := binary.Decode(buffer, binary.NativeEndian, &response); err != nil {
		return Pair{}, err
	}

	return response, nil
}
