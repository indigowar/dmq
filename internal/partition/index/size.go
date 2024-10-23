package index

import (
	"context"
	"os"
)

type SizeReq struct {
	Filename string `json:"filename"`
}

type SizeRes struct {
	Size int64 `json:"size"`
}

func Size(ctx context.Context, request SizeReq) (SizeRes, error) {
	stat, err := os.Stat(request.Filename)
	if err != nil {
		return SizeRes{}, err
	}

	return SizeRes{
		Size: stat.Size() / pairSize,
	}, nil
}
