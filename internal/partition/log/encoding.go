package log

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/indigowar/dmq/internal/core/record"
)

func encodeRecord(record record.Record) ([]byte, error) {
	buffer := new(bytes.Buffer)

	if err := binary.Write(buffer, binary.NativeEndian, record.Offset); err != nil {
		return nil, err
	}

	timestamp := record.Timestamp.UnixNano()
	if err := binary.Write(buffer, binary.NativeEndian, timestamp); err != nil {
		return nil, err
	}

	keySize := int64(len(record.Key))
	if err := binary.Write(buffer, binary.NativeEndian, keySize); err != nil {
		return nil, err
	}

	if keySize > 0 {
		if err := binary.Write(buffer, binary.NativeEndian, record.Key); err != nil {
			return nil, err
		}
	}

	valueLen := int64(len(record.Value))
	if err := binary.Write(buffer, binary.NativeEndian, valueLen); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.NativeEndian, record.Value); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func decodeRecord(data []byte) (record.Record, error) {
	buf := bytes.NewReader(data)
	r := record.Record{}

	if err := binary.Read(buf, binary.BigEndian, &r.Offset); err != nil {
		return record.Record{}, err
	}

	var timestamp int64
	if err := binary.Read(buf, binary.BigEndian, &timestamp); err != nil {
		return record.Record{}, err
	}
	r.Timestamp = time.Unix(0, timestamp)

	var keyLen int64
	if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
		return record.Record{}, err
	}

	if keyLen > 0 {
		r.Key = make([]byte, keyLen)
		if err := binary.Read(buf, binary.BigEndian, &r.Key); err != nil {
			return record.Record{}, err
		}
	}

	var valueLen int64
	if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
		return record.Record{}, err
	}

	r.Value = make([]byte, valueLen)
	if err := binary.Read(buf, binary.BigEndian, &r.Value); err != nil {
		return record.Record{}, err
	}

	return r, nil
}
