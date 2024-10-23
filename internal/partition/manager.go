package partition

import (
	"context"
	"sync"

	"github.com/indigowar/dmq/internal/topic"
)

type Manager struct {
	mutex      sync.RWMutex
	partitions []*partition

	index indexActions
	log   logActions
}

func (m *Manager) CreatePartition(ctx context.Context, request topic.NewPartitionRequest) (topic.NewPartitionResponse, error) {
	panic("unimported")
}

func (m *Manager) WritePartition(ctx context.Context, request topic.WriteIntoPartitionRequest) (topic.WriteIntoPartitionResponse, error) {
	panic("unimplemented")
}

func (m *Manager) ReadByOffset(ctx context.Context, request topic.ReadByOffsetFromPartitionRequest) (topic.ReadFromPartitionResponse, error) {
	panic("unimplemented")
}

func (m *Manager) ReadByTimestamp(ctx context.Context, request topic.ReadByTimestampFromPartitionRequest) (topic.ReadFromPartitionResponse, error) {
	panic("unimplemented")
}
