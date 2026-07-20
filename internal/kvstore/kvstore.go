package kvstore

import (
	"distributed_kv_store/internal/persistence"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

type KVStoreData struct {
	Data         map[string]string
	Mu           sync.RWMutex
	wal          *persistence.WAL
	RecoveryMode bool
}

func NewKVStore(wal *persistence.WAL) *KVStoreData {
	return &KVStoreData{
		Data: make(map[string]string, 1000),
		wal:  wal,
	}
}

func (kv *KVStoreData) Set(key, value string) error {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if !kv.RecoveryMode {
		if err := kv.wal.Set(key, value); err != nil {
			log.Printf("Failed to write SET to WAL: %v", err)
			return err
		}
	}

	kv.Data[key] = value
	return nil
}

func (kv *KVStoreData) Get(key string) (string, bool) {
	kv.Mu.RLock()
	defer kv.Mu.RUnlock()
	val, ok := kv.Data[key]
	return val, ok
}

func (kv *KVStoreData) Remove(key string) error {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if !kv.RecoveryMode {
		if err := kv.wal.Delete(key); err != nil {
			log.Printf("Failed to write DEL to WAL: %v", err)
			return err
		}
	}

	delete(kv.Data, key)
	return nil
}

func (kv *KVStoreData) Snapshot(path string) error {
	kv.Mu.Lock()
	data := make(map[string]string, len(kv.Data))
	for k, v := range kv.Data {
		data[k] = v
	}
	offset := kv.wal.Offset()
	kv.Mu.Unlock()

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode snapshot data: %w", err)
	}

	return kv.wal.TruncateTo(offset)
}
