package partition

import (
	"context"
	"sync"

	"github.com/indigowar/dmq/internal/partition/index"
	"github.com/indigowar/dmq/internal/partition/log"
	"github.com/indigowar/dmq/internal/topic"
)

type Manager struct {
	mutex      sync.RWMutex
	partitions []*partition

	index index.Index
	log   log.Log
}

func (m *Manager) CreatePartition(ctx context.Context, request topic.NewPartitionRequest) (topic.NewPartitionResponse, error) {
	panic("unimplemented")
}

func (m *Manager) Write(ctx context.Context, request topic.WriteIntoPartitionRequest) (topic.WriteIntoPartitionResponse, error) {
	panic("unimplemented")
}

func (m *Manager) ReadByOffset(ctx context.Context, request topic.ReadByOffsetFromPartitionRequest) (topic.ReadFromPartitionResponse, error) {
	panic("unimplemented")
}

func (m *Manager) ReadByTimestamp(ctx context.Context, request topic.ReadByTimestampFromPartitionRequest) (topic.ReadFromPartitionResponse, error) {
	panic("unimplemented")
}

func NewManager() (*Manager, error) {
	panic("unimplemented")
}
