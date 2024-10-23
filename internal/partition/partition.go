package partition

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/indigowar/dmq/internal/core/communication"
	"github.com/indigowar/dmq/internal/core/record"
	"github.com/indigowar/dmq/internal/partition/index"
	"github.com/indigowar/dmq/internal/partition/log"
)

type indexActions struct {
	Find   chan<- communication.Request[index.FindReq, index.FindRes]
	Insert chan<- communication.Request[index.InsertReq, index.InsertRes]
	Latest chan<- communication.Request[index.LatestReq, index.LatestRes]
	Size   chan<- communication.Request[index.SizeReq, index.SizeRes]
}

type logActions struct {
	Read  chan<- communication.Request[log.ReadReq, log.ReadRes]
	Write chan<- communication.Request[log.WriteReq, log.WriteRes]
}

type partition struct {
	logger *slog.Logger

	mutex sync.RWMutex

	index indexActions
	log   logActions

	Path       string  `json:"path"`
	Number     int64   `json:"number"`
	Logs       []int64 `json:"logs"`
	LogSize    int64   `json:"log_size"`
	NextOffset int64   `json:"next_offset"`
}

func (p *partition) Write(ctx context.Context, payload record.RecordCreationPayload) (int64, time.Time, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var segment int64

	// ....

	record := record.Record{
		Offset:    p.NextOffset,
		Timestamp: time.Now(),
		Key:       payload.Key,
		Value:     payload.Value,
	}

	logRes, err := communication.Sync(ctx, p.log.Write, log.WriteReq{
		Filename: logPath(p.Path, segment), Record: record,
	})
	if err != nil {
		return 0, time.Time{}, err
	}

	if _, err = communication.Sync(ctx, p.index.Insert, index.InsertReq{
		Filename: timestampIdxPath(p.Path, segment),
		Data:     index.Pair{Key: record.Timestamp.UnixNano(), Value: record.Offset},
	}); err != nil {
		return 0, time.Time{}, err
	}

	if _, err = communication.Sync(ctx, p.index.Insert, index.InsertReq{
		Filename: offsetIdxPath(p.Path, segment),
		Data: index.Pair{
			Key:   record.Offset,
			Value: logRes.PhysicalPosition,
		},
	}); err != nil {
		return 0, time.Time{}, err
	}

	p.NextOffset++

	return record.Offset, record.Timestamp, nil
}

func (p *partition) ReadByOffset(ctx context.Context, offset int64) (record.Record, error) {
	panic("unimplemented")
}

func (p *partition) ReadByTimestamp(ctx context.Context, timestamp time.Time) (record.Record, error) {
	panic("unimplemented")
}

func initPartition() (*partition, error) { panic("unimplemented") }

func loadPartition() (*partition, error) { panic("unimplemented") }

func logPath(path string, number int64) string {
	return fmt.Sprintf("%s/%08d.part", path, number)
}

func offsetIdxPath(path string, number int64) string {
	return fmt.Sprintf("%s/%08d.idx", path, number)
}

func timestampIdxPath(path string, number int64) string {
	return fmt.Sprintf("%s/%08d.tdx", path, number)
}
