package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/indigowar/dmq/internal/core/record"
	"github.com/indigowar/dmq/internal/partition/log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := log.InitLog(ctx, 4)

	file, pos, err := l.WriteNew(context.Background(), "/tmp/dmq", "log", record.Record{
		Offset:    0,
		Timestamp: time.Now(),
		Key:       []byte{},
		Value:     []byte("Hello, world, how are you"),
	})

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Printf("file: %d, pos: %d", file, pos)
}

// var (
// 	mutex   = sync.Mutex{}
// 	records = make([]events.Record, 0)
// )
//
// func insertRecord(ctx context.Context, payload events.InsertRecordInPartition) (events.RecordInsertedInPartition, error) {
// 	mutex.Lock()
// 	defer mutex.Unlock()
//
// 	offset := int64(len(records))
//
// 	r := events.Record{
// 		Offset:    offset,
// 		Timestamp: time.Now(),
// 		Key:       payload.Data.Key,
// 		Value:     payload.Data.Value,
// 	}
//
// 	records = append(records, r)
//
// 	return events.RecordInsertedInPartition{
// 		Partition: 0,
// 		Record:    r,
// 	}, nil
// }

// func other() {
// 	fmt.Println("hello")
//
// 	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
// 	defer cancel()
//
// 	requests := runner.CreateRunner(ctx, insertRecord)
//
// 	for i := 0; i != 10; i++ {
// 		out := make(chan events.RecordInsertedInPartition)
// 		err := make(chan error)
//
// 		requests <- runner.Request[events.InsertRecordInPartition, events.RecordInsertedInPartition]{
// 			Input: events.InsertRecordInPartition{
// 				Partition: 0,
// 				Data: events.RecordCreationContent{
// 					Key:   "hi",
// 					Value: "hello",
// 				},
// 			},
// 			Output: out,
// 			Error:  err,
// 		}
//
// 		select {
// 		case <-out:
// 		case <-err:
// 		}
//
// 		time.Sleep(1 * time.Second)
// 	}
//
// 	<-ctx.Done()
// 	close(requests)
//
// 	json.NewEncoder(os.Stdout).Encode(records)
// }
