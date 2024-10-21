package storage

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/indigowar/dmq/internal/core"
	"github.com/lmittmann/tint"
)

func Example() {
	initDir()

	logger := slog.New(tint.NewHandler(os.Stdout, nil))

	seg, err := NewSegment(logger, "/tmp/dmq/", 0)
	if err != nil {
		logger.Error("failed to create a segment", "err", err)
		return
	}
	defer seg.Close()

	for i := 0; i != 10; i++ {
		if err := seg.Write(context.Background(), core.NewMessage(int64(i), time.Now(), nil, randomByteString(32))); err != nil {
			logger.Error("failed to write", "err", err)
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i != 10; i++ {
		msg, err := seg.ReadByOffset(context.Background(), int64(i))
		if err != nil {
			logger.Error("failed to read", "err", err)
			return
		}

		logger.Info(
			"Read a message from a segment file",
			"offset", msg.Offset,
			"timestamp", msg.Timestamp.String(),
		)
	}

	fo, lo, fts, lts := seg.Stats()
	logger.Info(
		"Segment statistics",
		"first offset", fo,
		"last offset", lo,
		"first timestamp", fts,
		"last timestamp", lts,
	)

	logger.Info("segment works")

	{
		p, err := NewPartition(logger, "/tmp/dmq", 0, 32)
		if err != nil {
			logger.Error("failed to create a partition", "err", err)
			return
		}
		defer p.Close()

		p.DumpData()

		logger.Info("partition creates")
	}

	{
		p, err := LoadPartition(logger, "/tmp/dmq", 0)
		if err != nil {
			logger.Error("failed to load a partition", "err", err)
			return
		}
		defer p.Close()

		p.DumpData()

		logger.Info("partition loads")
	}

	{
		p, err := NewPartition(logger, "/tmp/dmq", 1, 32)
		if err != nil {
			logger.Error("failed to create a partition", "err", err)
			return
		}
		defer p.Close()

		p.DumpData()

		logger.Info("partition creates")
	}
}

func initDir() {
	path := "/tmp/dmq"

	_, err := os.Stat(path)
	if !os.IsNotExist(err) {
		os.RemoveAll(path)
	}

	os.MkdirAll(path, os.ModePerm)
}

func randomByteString(n int) []byte {
	result := make([]byte, n)
	io.ReadFull(rand.Reader, result)
	return result
}
