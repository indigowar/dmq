package index

import (
	"context"
	"os"
)

type statRequest struct {
	Filename string `json:"filename"`
}

type Stat struct {
	Size int64 `json:"size"`
}

func stat(ctx context.Context, request statRequest) (Stat, error) {
	stat, err := os.Stat(request.Filename)
	if err != nil {
		return Stat{}, err
	}

	return Stat{
		Size: stat.Size() / pairSize,
	}, nil
}
