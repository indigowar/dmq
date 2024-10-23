package index

import (
	"context"
	"encoding/binary"
	"io"
	"os"
)

type LatestReq struct {
	Filename string `json:"filename"`
}

type LatestRes = Pair

func Latest(ctx context.Context, request LatestReq) (LatestRes, error) {
	file, err := os.OpenFile(request.Filename, os.O_RDONLY, 0644)
	if err != nil {
		return LatestRes{}, err
	}
	defer file.Close()

	if _, err := file.Seek(io.SeekEnd, pairSize); err != nil {
		return LatestRes{}, err
	}

	buffer := make([]byte, pairSize)
	if _, err := file.Read(buffer); err != nil {
		return LatestRes{}, err
	}

	var response LatestRes
	if _, err := binary.Decode(buffer, binary.NativeEndian, &response); err != nil {
		return LatestRes{}, err
	}

	return response, nil
}
