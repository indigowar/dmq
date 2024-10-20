package core

import (
	"encoding/binary"
	"errors"
	"io"
	"time"
)

type Message struct {
	Offset    int64
	Size      int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
}

func NewMessage(offset int64, ts time.Time, key []byte, value []byte) Message {
	return Message{
		Offset:    offset,
		Size:      int64(len(value)),
		Timestamp: ts,
		Key:       key,
		Value:     value,
	}
}

func (msg *Message) EncodeBinary(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, msg.Offset); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, msg.Size); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, msg.Timestamp.UnixNano()); err != nil {
		return err
	}

	var keyLen int64

	if msg.Key != nil {
		keyLen = int64(len(msg.Key))
	}

	if err := binary.Write(w, binary.LittleEndian, keyLen); err != nil {
		return err
	}

	if keyLen > 0 {
		if _, err := w.Write(msg.Key); err != nil {
			return err
		}
	}

	valueLen := int64(len(msg.Value))

	if err := binary.Write(w, binary.LittleEndian, valueLen); err != nil {
		return err
	}

	if _, err := w.Write(msg.Value); err != nil {
		return err
	}

	return nil
}

func DecodeBinaryMessage(r io.Reader) (Message, error) {
	var msg Message

	if err := binary.Read(r, binary.LittleEndian, &msg.Offset); err != nil {
		return msg, err
	}

	if err := binary.Read(r, binary.LittleEndian, &msg.Size); err != nil {
		return msg, err
	}

	var ts int64
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return msg, err
	}
	msg.Timestamp = time.Unix(0, ts)

	var keyLen int64
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return msg, err
	}
	if keyLen > 0 {
		msg.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(r, msg.Key); err != nil {
			return msg, err
		}
	} else {
		msg.Key = nil
	}

	var valueLen int64
	if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
		return msg, err
	}

	if valueLen <= 0 {
		return Message{}, errors.New("value is invalid")
	}

	msg.Value = make([]byte, valueLen)
	if _, err := io.ReadFull(r, msg.Value); err != nil {
		return msg, err
	}

	return msg, nil
}
