package kvstore

import "sync"

type KVStoreData struct {
	Data map[string]string
	Mu   sync.Mutex
}


func NewKVStore() *KVStoreData {
	return &KVStoreData{
		Data: make(map[string]string),
	}
}

func (kv *KVStoreData) Set(key string, value string) {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()
	kv.Data[key] = value
    return
}

func (kv *KVStoreData) Get(key string) (string, bool) {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()
	val, ok := kv.Data[key]
	return val, ok
}

func (kv *KVStoreData) Remove(key string) {
	kv.Mu.Lock()
    defer kv.Mu.Unlock()
    delete(kv.Data, key)
    return
}

