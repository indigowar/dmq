package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sync"
)

type partitionData struct {
	Number      int64   `json:"number"`
	SegmentSize int64   `json:"segment_size"`
	NextOffset  int64   `json:"next"`
	Segments    []int64 `json:"segments"`
}

type Partition struct {
	logger *slog.Logger

	mutex sync.RWMutex
	data  partitionData

	segments []*Segment

	partitionPath string
	basePath      string
}

func (p *Partition) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, s := range p.segments {
		s.Close()
	}
}

func (p *Partition) DumpData() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	pFile, err := os.OpenFile(p.partitionPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer pFile.Close()

	if err := json.NewEncoder(pFile).Encode(p.data); err != nil {
		return err
	}

	return nil
}

func NewPartition(logger *slog.Logger, basePath string, number int64, segmentSize int64) (*Partition, error) {
	logger.Info("Partition is being created...", "basePath", basePath, "number", number, "segmentSize", segmentSize)

	partitionPath := fmt.Sprintf("%s/%08d.part", basePath, number)

	if _, err := os.Stat(partitionPath); !errors.Is(err, fs.ErrNotExist) {
		logger.Error("Couldn't create new partition, it already exists", "partitionPath", partitionPath, "err", err)
		return nil, errors.New("already exists")
	}

	segmentNumber, err := nextFreeSegmentNumber(basePath)
	if err != nil {
		logger.Error("Couldn't receive next free segment number", "basePath", basePath, "err", err)
		return nil, err
	}

	logger.Info("Creating a new segment", "number", segmentNumber)

	segment, err := NewSegment(logger, basePath, segmentNumber)
	if err != nil {
		logger.Error("Couldn't create a new segment", "basePath", basePath, "number", segmentNumber, "err", err)
		return nil, err
	}

	p := &Partition{
		logger:        logger,
		mutex:         sync.RWMutex{},
		data:          partitionData{Number: number, SegmentSize: segmentSize, NextOffset: 0, Segments: []int64{segmentNumber}},
		segments:      []*Segment{segment},
		partitionPath: partitionPath,
		basePath:      basePath,
	}

	if err := p.DumpData(); err != nil {
		logger.Error("Couldn't dump partition data", "reason", "init", "basePath", p.basePath, "number", p.data.Number, "err", err)
		return nil, err
	}

	return p, nil
}

func LoadPartition(logger *slog.Logger, basePath string, number int64) (*Partition, error) {
	logger.Info("Partition is being loaded...", "basePath", basePath, "number", number)

	partitionPath := fmt.Sprintf("%s/%08d.part", basePath, number)

	pFile, err := os.OpenFile(partitionPath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer pFile.Close()

	var pData partitionData
	if err := json.NewDecoder(pFile).Decode(&pData); err != nil {
		return nil, err
	}

	segments := make([]*Segment, 0, len(pData.Segments))

	for _, s := range pData.Segments {
		segment, err := NewSegment(logger, basePath, s)
		if err != nil {
			return nil, err
		}
		segments = append(segments, segment)
	}

	return &Partition{
		logger:        logger,
		mutex:         sync.RWMutex{},
		data:          pData,
		segments:      segments,
		partitionPath: partitionPath,
		basePath:      basePath,
	}, nil
}
