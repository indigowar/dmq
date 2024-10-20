package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/indigowar/dmq/internal/core"
)

type Segment struct {
	logger *slog.Logger
	number int64

	mutex sync.RWMutex

	log            *os.File
	offsetIndex    *os.File
	timestampIndex *os.File

	firstOffset    int64
	lastOffset     int64
	firstTimestamp time.Time
	lastTimestamp  time.Time
}

func (segment *Segment) ReadByOffset(ctx context.Context, offset int64) (core.Message, error) {
	segment.mutex.RLock()
	defer segment.mutex.RUnlock()

	position, err := readIndex(ctx, segment.offsetIndex, offset)
	if err != nil {
		return core.Message{}, err
	}

	return readFromLog(segment.log, position)
}

func (segment *Segment) ReadByTimestamp(ctx context.Context, timestamp time.Time) (core.Message, error) {
	segment.mutex.RLock()
	defer segment.mutex.RUnlock()

	logicalOffset, err := readIndex(ctx, segment.timestampIndex, timestamp.UnixNano())
	if err != nil {
		return core.Message{}, err
	}

	position, err := readIndex(ctx, segment.offsetIndex, logicalOffset)
	if err != nil {
		return core.Message{}, err
	}

	return readFromLog(segment.log, position)
}

func (segment *Segment) Write(ctx context.Context, msg core.Message) error {
	segment.mutex.Lock()
	defer segment.mutex.Unlock()

	pos, err := writeIntoLog(segment.log, msg)
	if err != nil {
		return err
	}

	if segment.firstOffset == -1 {
		segment.firstOffset = msg.Offset
		segment.firstTimestamp = msg.Timestamp
	}

	segment.lastOffset = msg.Offset
	segment.lastTimestamp = msg.Timestamp

	return errors.Join(
		writeIntoIndex(segment.offsetIndex, indexPair{Key: msg.Offset, Value: pos}),
		writeIntoIndex(segment.timestampIndex, indexPair{Key: msg.Timestamp.UnixNano(), Value: msg.Offset}),
	)
}

func (segment *Segment) Stats() (int64, int64, time.Time, time.Time) {
	segment.mutex.RLock()
	defer segment.mutex.RUnlock()

	return segment.firstOffset, segment.lastOffset, segment.firstTimestamp, segment.lastTimestamp
}

func (segment *Segment) Close() error {
	segment.mutex.Lock()
	defer segment.mutex.Unlock()

	return errors.Join(
		segment.log.Close(),
		segment.offsetIndex.Close(),
		segment.timestampIndex.Close(),
	)
}

func NewSegment(logger *slog.Logger, basePath string, segmentNumber int64) (*Segment, error) {
	commonPath := fmt.Sprintf("%s/%08d", basePath, segmentNumber)
	logPath := commonPath + ".log"
	offsetIndexPath := commonPath + ".of_idx"
	timestampIndexPath := commonPath + ".ts_idx"

	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open a log file: %w", err)
	}

	offsetIndex, err := os.OpenFile(offsetIndexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open an offset index file: %w", err)
	}

	timestampIndex, err := os.OpenFile(timestampIndexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open an timestamp index file: %w", err)
	}

	return &Segment{
		logger:         logger,
		number:         segmentNumber,
		mutex:          sync.RWMutex{},
		log:            log,
		offsetIndex:    offsetIndex,
		timestampIndex: timestampIndex,
		firstOffset:    -1,
		lastOffset:     -1,
		firstTimestamp: time.Time{},
		lastTimestamp:  time.Time{},
	}, nil
}

func nextFreeSegmentNumber(basePath string) (int64, error) {
	logFileRegex := regexp.MustCompile(`^(\d{8})\.log$`)

	entries, err := os.ReadDir(basePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory: %w", err)
	}

	maxNumber := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()

		matches := logFileRegex.FindStringSubmatch(fileName)
		if matches == nil {
			continue
		}

		number, err := strconv.Atoi(matches[1])
		if err != nil {
			return 0, fmt.Errorf("failed to parse number from file %s: %w", fileName, err)
		}

		if number > maxNumber {
			maxNumber = number
		}
	}

	if maxNumber == 0 {
		return 1, nil
	}

	return int64(maxNumber + 1), nil
}
