package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"unsafe"
)

type indexPair struct {
	Key   int64
	Value int64
}

const indexPairSize = int64(unsafe.Sizeof(indexPair{}))

// writeIntoIndex - writes into the index file an index pair.
func writeIntoIndex(file *os.File, pair indexPair) error {
	data := bytes.NewBuffer(make([]byte, indexPairSize))

	err := binary.Write(data, binary.NativeEndian, pair)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	_, err = file.WriteAt(data.Bytes(), stat.Size())

	return err
}

// readIndex - reads from an index file and returns a value of the given key.
//
// Important: readIndex HAS TO BE concurrently safe! Due to multiple use of it in [Segment]
func readIndex(ctx context.Context, file *os.File, key int64) (int64, error) {
	var offset int64 = 0
	buffer := make([]byte, indexPairSize)

	for {
		select {
		case <-ctx.Done():
			return 0, errors.New("context is cancelled")
		default:
			n, err := file.ReadAt(buffer, offset)
			if err != nil {
				return 0, err
			}

			if int64(n) != indexPairSize {
				return 0, errors.New("read invalid amount of bytes")
			}

			var pair indexPair
			if _, err := binary.Decode(buffer, binary.NativeEndian, &pair); err != nil {
				return 0, err
			}

			if pair.Key == key {
				return pair.Value, nil
			}

			offset += indexPairSize
		}
	}
}
