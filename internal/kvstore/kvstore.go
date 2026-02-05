package kvstore

import "sync"

type kvstoreData struct {
    data map[string]string
    mu   sync.Mutex
}

func (kvstore *kvstoreData) set(key string, value string) {
    kvstore.mu.Lock()
    defer kvstore.mu.Unlock()
    
    kvstore.data[key] = value
}

func (kvstore *kvstoreData) get(key string) (string, bool) {
    kvstore.mu.Lock()
    defer kvstore.mu.Unlock()
    
    val, ok := kvstore.data[key]
    return val, ok
}

func (kvstore *kvstoreData) remove(key string) bool {
    kvstore.mu.Lock()
    defer kvstore.mu.Unlock()
    
    if _, ok := kvstore.data[key]; ok {
        delete(kvstore.data, key)
        return true
    }
    return false
}