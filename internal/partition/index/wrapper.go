package index

import (
	"context"

	"github.com/indigowar/dmq/internal/core/communication"
)

type Index struct {
	find   chan<- communication.Request[findRequest, findResponse]
	insert chan<- communication.Request[insertRequest, noResponse]
	latest chan<- communication.Request[latestRequest, Pair]
	stat   chan<- communication.Request[statRequest, Stat]
}

func (idx Index) Find(ctx context.Context, filename string, key int64) (int64, error) {
	res, err := communication.Sync(ctx, idx.find, findRequest{
		Filename: filename,
		Key:      key,
	})

	if err != nil {
		return 0, err
	}

	return res.Value, nil
}

func (idx Index) Insert(ctx context.Context, filename string, data Pair) error {
	_, err := communication.Sync(ctx, idx.insert, insertRequest{
		Filename: filename,
		Data:     data,
	})
	return err
}

func (idx Index) Latest(ctx context.Context, filename string) (Pair, error) {
	return communication.Sync(ctx, idx.latest, latestRequest{Filename: filename})
}

func (idx Index) Stat(ctx context.Context, filename string) (Stat, error) {
	return communication.Sync(ctx, idx.stat, statRequest{Filename: filename})
}

func InitIndex(ctx context.Context, workersPerOperation int64) Index {
	return Index{
		find:   communication.Workers(ctx, find, int(workersPerOperation)),
		insert: communication.Workers(ctx, Insert, int(workersPerOperation)),
		latest: communication.Workers(ctx, latest, int(workersPerOperation)),
		stat:   communication.Workers(ctx, stat, int(workersPerOperation)),
	}

}
