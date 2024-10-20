package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"unsafe"

	"github.com/indigowar/dmq/internal/core"
)

type logHeader struct {
	Size int64
}

const logHeaderSize = int64(unsafe.Sizeof(logHeader{}))

func readFromLog(file *os.File, position int64) (core.Message, error) {
	headerBuffer := make([]byte, logHeaderSize)

	n, err := file.ReadAt(headerBuffer, position)
	if err != nil {
		return core.Message{}, fmt.Errorf("failed to read the header from log file: %w", err)
	}

	if int64(n) != logHeaderSize {
		return core.Message{}, errors.New("failed to read the header from log file: read less bytes then expected of the header")
	}

	var header logHeader
	if _, err := binary.Decode(headerBuffer, binary.NativeEndian, &header); err != nil {
		return core.Message{}, fmt.Errorf("failed to decode the header: %w", err)
	}

	messageBuffer := make([]byte, header.Size)
	n, err = file.ReadAt(messageBuffer, position+logHeaderSize)
	if err != nil {
		return core.Message{}, fmt.Errorf("failed to read the message from log file: %w", err)
	}

	if int64(n) != header.Size {
		return core.Message{}, errors.New("failed to read the message from log file: read less bytes then expected of the header")
	}

	msg, err := core.DecodeBinaryMessage(bytes.NewBuffer(messageBuffer))
	if err != nil {
		return core.Message{}, fmt.Errorf("failed to decode the message: %w", err)
	}

	return msg, nil
}

func writeIntoLog(log *os.File, message core.Message) (int64, error) {
	stat, err := log.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to receive stat of the file: %w", err)
	}

	var msgBuffer bytes.Buffer

	if err := message.EncodeBinary(&msgBuffer); err != nil {
		return 0, fmt.Errorf("failed to encode the message: %w", err)
	}

	header := make([]byte, logHeaderSize)
	if _, err := binary.Encode(header, binary.NativeEndian, &logHeader{Size: int64(msgBuffer.Len())}); err != nil {
		return 0, fmt.Errorf("failed to encode the header: %w", err)
	}

	pos := stat.Size()

	if _, err := log.WriteAt(header, pos); err != nil {
		return 0, fmt.Errorf("failed to write the header into the log: %w", err)
	}

	if _, err := log.WriteAt(msgBuffer.Bytes(), pos+logHeaderSize); err != nil {
		return 0, fmt.Errorf("failed to write the message into the log: %w", err)
	}

	return pos, nil
}
