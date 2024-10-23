package topic

import "time"

// NewPartitionRequest - is used to request a creation of a new partition
type NewPartitionRequest = struct{}

// NewPartitionResponse - is used as a return value for [NewPartitionResponse],
// It contains an ID of a newly created partition.
type NewPartitionResponse struct {
	Partition int64 `json:"int"`
}

// WriteIntoPartitionRequest - is used to request write operation into a partition
type WriteIntoPartitionRequest struct {
	Partition int64  `json:"partition"`
	Key       []byte `json:"key,omitempty"`
	Value     []byte `json:"value"`
}

// WriteIntoPartitionResponse - is used as a return value for [WriteIntoPartitionRequest]
type WriteIntoPartitionResponse struct {
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

type ReadByOffsetFromPartitionRequest struct {
	Partition int64 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type ReadByTimestampFromPartitionRequest struct {
	Partition int64     `json:"partition"`
	Timestamp time.Time `json:"timestamp"`
}

type ReadFromPartitionResponse struct {
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Key       []byte    `json:"key,omitempty"`
	Value     []byte    `json:"value"`
}
