package simple

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

type Message struct {
	Offset    int       `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value"`
}

type MessageCreatePayload struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value"`
}

type Partition interface {
	Put(context.Context, MessageCreatePayload) (int, error)
	Get(context.Context, int) (Message, error)
}

type memPartition struct {
	mutex sync.Mutex
	msgs  []Message
}

var _ Partition = &memPartition{}

// Get implements Partition.
func (m *memPartition) Get(_ context.Context, offset int) (Message, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.msgs) < offset {
		return Message{}, errors.New("offset is invalid")
	}

	return m.msgs[offset], nil
}

// Put implements Partition.
func (m *memPartition) Put(_ context.Context, mcp MessageCreatePayload) (int, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	offset := len(m.msgs)

	m.msgs = append(m.msgs, Message{
		Offset:    offset,
		Timestamp: time.Now(),
		Key:       mcp.Key,
		Value:     mcp.Value,
	})

	return offset, nil
}

type StorageController struct {
	mutex      sync.Mutex
	partitions map[string]Partition
}

func (sc *StorageController) Get(ctx context.Context, topic string, offset int) (Message, error) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	p, found := sc.partitions[topic]
	if !found {
		return Message{}, errors.New("topic is not found")
	}
	return p.Get(ctx, offset)
}

func (sc *StorageController) Put(ctx context.Context, topic string, payload MessageCreatePayload) (int, error) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	p, found := sc.partitions[topic]
	if !found {
		p = &memPartition{
			mutex: sync.Mutex{},
			msgs:  make([]Message, 0),
		}
		sc.partitions[topic] = p
	}

	return p.Put(ctx, payload)
}

func handleGet(sc *StorageController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		offsetStr := r.PathValue("offset")

		offset, err := strconv.Atoi(offsetStr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid offset"})
			return
		}

		msg, err := sc.Get(r.Context(), topic, offset)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(msg)
	}
}

func handlePut(sc *StorageController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")

		var payload MessageCreatePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		offset, err := sc.Put(r.Context(), topic, payload)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]int{"offset": offset})
	}
}

func Simple() {
	sc := &StorageController{
		mutex:      sync.Mutex{},
		partitions: make(map[string]Partition),
	}

	mux := http.NewServeMux()

	mux.Handle("GET /{topic}/{offset}", handleGet(sc))
	mux.Handle("PUT /{topic}", handlePut(sc))

	server := &http.Server{
		Addr:    ":8000",
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
