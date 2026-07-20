package server

import (
	"context"
	"distributed_kv_store/internal/kvstore"
	"distributed_kv_store/internal/persistence"
	"log"
	"sync"
)

type KVStoreService struct {
    UnimplementedKvStoreServer
    Store *kvstore.KVStoreData
    Wal *persistence.WAL
    Counter *OperationCounter
    SnapManager *SnapshotManager
}

type OperationCounter struct {
    count int
    mu    sync.Mutex
}

type SnapshotManager struct {
    snapshotInProgress bool
    mu                 sync.Mutex
}

func (s *KVStoreService) GetHandler(ctx context.Context, req *GetRequest) (*GetResponse, error) {
    val, ok  := s.Store.Get(req.Key)
   
    return &GetResponse{
        Found: ok,
        Value: val,
    }, nil
}

func (s *KVStoreService) SetHandler(ctx context.Context, req *SetRequest) (*SetResponse, error) {
    s.Store.Set(req.Key, req.Value)

    // Increment the counter for set operations
    go func() {
        if IncrementCounter(s.Counter) {
            s.SnapManager.TriggerSnapshot(s.Store, s.Wal)
        }
    }()
    
    return &SetResponse{
        Success: true,
    }, nil
}

func (s *KVStoreService) DeleteHandler(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {

    s.Store.Remove(req.Key)
    // Increment the counter for delete operations
    go func() {
        if IncrementCounter(s.Counter) {
            s.SnapManager.TriggerSnapshot(s.Store, s.Wal)
        }
    }()
    
    return &DeleteResponse{
        Success: true,
    } , nil
}

// IncrementCounter increments the operation counter and returns true if it reaches the threshold
func IncrementCounter(counter *OperationCounter) bool {
    counter.mu.Lock()
    defer counter.mu.Unlock()
    counter.count++
    if counter.count >= 1000 {
        counter.count = 0
        return true
    }
    return false
}


func (s *SnapshotManager) TriggerSnapshot(store *kvstore.KVStoreData, wal *persistence.WAL) {
    s.mu.Lock()
    if s.snapshotInProgress {
        s.mu.Unlock()
        return
    }
    s.snapshotInProgress = true
    s.mu.Unlock()

    go func() {
        if err := persistence.SaveSnapshot("snapshot.json", store, wal); err != nil {
            log.Printf("Failed to save snapshot: %v", err)
        } else {
            log.Println("Snapshot saved successfully")
        }

        s.mu.Lock()
        s.snapshotInProgress = false
        s.mu.Unlock()
    }()
}
