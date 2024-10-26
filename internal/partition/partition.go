package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/indigowar/dmq/internal/core/record"
	"github.com/indigowar/dmq/internal/partition/index"
	"github.com/indigowar/dmq/internal/partition/log"
)

var (
	partExt         = "part"
	logExt          = "log"
	offsetIdxExt    = "oidx"
	timestampIdxExt = "tdx"
)

type partition struct {
	logger *slog.Logger

	mutex sync.RWMutex

	index index.Index
	log   log.Log

	path string

	Number     int64   `json:"number"`
	Logs       []int64 `json:"logs"`
	LogSize    int64   `json:"log_size"`
	NextOffset int64   `json:"next_offset"`
}

func (p *partition) Write(ctx context.Context, payload record.RecordCreationPayload) (int64, time.Time, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	defer p.dump()

	record := record.Record{
		Offset:    p.NextOffset,
		Timestamp: time.Now(),
		Key:       payload.Key,
		Value:     payload.Value,
	}
	p.NextOffset++

	if len(p.Logs) == 0 {
		return record.Offset, record.Timestamp, p.writeNew(ctx, record)
	}

	number := p.Logs[len(p.Logs)-1]
	stat, err := p.index.Stat(ctx, p.offsetIndexPath(number))
	if err != nil {
		return 0, time.Time{}, err
	}

	if stat.Size == p.LogSize {
		return record.Offset, record.Timestamp, p.writeNew(ctx, record)
	}

	return record.Offset, record.Timestamp, p.write(ctx, record, number)
}

func (p *partition) writeNew(ctx context.Context, record record.Record) error {
	log, physicalPosition, err := p.log.WriteNew(ctx, p.path, logExt, record)
	if err != nil {
		return err
	}

	p.Logs = append(p.Logs, log)

	if err := p.index.Insert(ctx, p.timestampIndexPath(log), index.Pair{
		Key:   record.Timestamp.UnixNano(),
		Value: record.Offset,
	}); err != nil {
		return err
	}

	if err := p.index.Insert(ctx, p.offsetIndexPath(log), index.Pair{
		Key:   record.Offset,
		Value: physicalPosition,
	}); err != nil {
		return err
	}

	return nil
}

func (p *partition) write(ctx context.Context, record record.Record, log int64) error {
	physicalPosition, err := p.log.Write(ctx, p.logPath(log), record)
	if err != nil {
		return err
	}

	if err := p.index.Insert(ctx, p.timestampIndexPath(log), index.Pair{
		Key:   record.Timestamp.UnixNano(),
		Value: record.Offset,
	}); err != nil {
		return err
	}

	if err := p.index.Insert(ctx, p.offsetIndexPath(log), index.Pair{
		Key:   record.Offset,
		Value: physicalPosition,
	}); err != nil {
		return err
	}

	return nil
}

func (p *partition) ReadByOffset(ctx context.Context, offset int64) (record.Record, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	for _, log := range p.Logs {
		pos, err := p.index.Find(ctx, p.offsetIndexPath(log), offset)
		if err != nil {
			if err == io.EOF {
				continue
			}

			return record.Record{}, err
		}

		r, err := p.log.Read(ctx, p.logPath(log), pos)
		if err != nil {
			return record.Record{}, err
		}

		return r, nil
	}

	return record.Record{}, errors.New("not found")
}

func (p *partition) ReadByTimestamp(ctx context.Context, timestamp time.Time) (record.Record, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	for _, log := range p.Logs {
		offset, err := p.index.Find(ctx, p.timestampIndexPath(log), timestamp.UnixNano())
		if err != nil {
			if err == io.EOF {
				continue
			}

			return record.Record{}, err
		}

		pos, err := p.index.Find(ctx, p.offsetIndexPath(log), offset)
		if err != nil {
			return record.Record{}, err
		}

		r, err := p.log.Read(ctx, p.logPath(log), pos)
		if err != nil {
			return record.Record{}, err
		}

		return r, nil
	}

	return record.Record{}, errors.New("not found")
}

func (p *partition) dump() error {
	file, err := os.OpenFile(p.partPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(p)
}

func (p *partition) offsetIndexPath(n int64) string {
	return fmt.Sprintf("%s/%08d.%s", p.path, n, offsetIdxExt)
}

func (p *partition) timestampIndexPath(number int64) string {
	return fmt.Sprintf("%s/%08d.%s", p.path, number, timestampIdxExt)
}

func (p *partition) logPath(number int64) string {
	return fmt.Sprintf("%s/%08d.%s", p.path, number, logExt)
}

func (p *partition) partPath() string {
	return fmt.Sprintf("%s/%08d.%s", p.path, p.Number, partExt)
}
