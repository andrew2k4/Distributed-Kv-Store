package kvstore

import (
	"distributed_kv_store/internal/persistence"
	"log"
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
	// Write to WAL only if not in recovery mode
	if !kv.RecoveryMode {
		if err := kv.wal.Set(key, value); err != nil {
			log.Printf("Failed to write SET to WAL: %v", err)
			return err
		}
	}

	kv.Mu.Lock()
	defer kv.Mu.Unlock()
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
	// Delete from WAL only if not in recovery mode
	if !kv.RecoveryMode {
		if err := kv.wal.Delete(key); err != nil {
			log.Printf("Failed to write DEL to WAL: %v", err)
			return err
		}
	}

	kv.Mu.Lock()
	defer kv.Mu.Unlock()
	delete(kv.Data, key)
	return nil
}

func (kv *KVStoreData) GetAllData() map[string]string {
	kv.Mu.RLock()
	defer kv.Mu.RUnlock()

	copy := make(map[string]string, len(kv.Data))
	for k, v := range kv.Data {
		copy[k] = v
	}
	return copy
}
