package index

import (
	"context"
	"encoding/binary"
	"os"
)

type FindReq struct {
	Filename string `json:"filename"`
	Key      int64  `json:"key"`
}

type FindRes struct {
	Value int64 `json:"value"`
}

func Find(ctx context.Context, request FindReq) (FindRes, error) {
	file, err := os.OpenFile(request.Filename, os.O_RDONLY, 0644)
	if err != nil {
		return FindRes{}, err
	}
	defer file.Close()

	position := int64(0)
	buffer := make([]byte, pairSize)
	for {
		if _, err := file.ReadAt(buffer, position); err != nil {
			return FindRes{}, err
		}

		var pair Pair
		if _, err := binary.Decode(buffer, binary.NativeEndian, &pair); err != nil {
			return FindRes{}, err
		}

		if pair.Key == request.Key {
			return FindRes{Value: pair.Value}, nil
		}
	}
}
