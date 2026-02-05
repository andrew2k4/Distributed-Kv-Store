package server

import (
	"context"
	"distributed_kv_store/internal/kvstore" // Import correct
)

type KVStoreService struct {
    UnimplementedKvStoreServer
    Store *kvstore.KVStoreData 
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

    return &SetResponse{
        Success: true,
    }, nil
}

func (s *KVStoreService) DeleteHandler(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
    s.Store.Remove(req.Key)
    return &DeleteResponse{
        Success: true,
    } , nil
}

