package record

import "time"

type Record struct {
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Key       []byte    `json:"key,omitempty"`
	Value     []byte    `json:"value"`
}

type RecordCreationPayload struct {
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value"`
}
