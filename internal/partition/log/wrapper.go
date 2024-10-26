package log

import (
	"context"

	"github.com/indigowar/dmq/internal/core/communication"
	"github.com/indigowar/dmq/internal/core/record"
)

type Log struct {
	read     chan<- communication.Request[readRequest, readResponse]
	write    chan<- communication.Request[writeRequest, writeResponse]
	writeNew chan<- communication.Request[writeInNewFileRequest, writeInNewFileResponse]
}

func (log Log) Read(ctx context.Context, filename string, position int64) (record.Record, error) {
	return communication.Sync(ctx, log.read, readRequest{
		Filename: filename,
		Position: position,
	})
}

func (log Log) Write(ctx context.Context, filename string, record record.Record) (int64, error) {
	result, err := communication.Sync(ctx, log.write, writeRequest{
		Filename: filename,
		Record:   record,
	})

	return result.PhysicalPosition, err
}

func (log Log) WriteNew(ctx context.Context, dir string, ext string, record record.Record) (int64, int64, error) {
	result, err := communication.Sync(ctx, log.writeNew, writeInNewFileRequest{
		Dir:    dir,
		Ext:    ext,
		Record: record,
	})
	if err != nil {
		return 0, 0, err
	}

	return int64(result.File), result.PhysicalPosition, nil
}

func InitLog(ctx context.Context, workersPerOperation int) Log {
	return Log{
		read:     communication.Workers(ctx, read, workersPerOperation),
		write:    communication.Workers(ctx, write, workersPerOperation),
		writeNew: communication.Workers(ctx, writeNew, workersPerOperation),
	}
}
