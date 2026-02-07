package main

import (
	"distributed_kv_store/internal/kvstore"
	"distributed_kv_store/internal/persistence"
	"distributed_kv_store/internal/server"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	path := "wal.log"

	// Initialize WAL
	walEngine, err := persistence.NewWAL(path)
	if err != nil {
		log.Fatalf("Failed to start WAL: %v", err)
	}

	// Initialize KV store
	kvStoreEngine := kvstore.NewKVStore(walEngine)

	// Enable recovery mode to avoid rewriting WAL during recovery
	kvStoreEngine.RecoveryMode = true
	err = walEngine.Recovery(func(operation, key, value string) {
			switch operation {
			case "SET":
			kvStoreEngine.Set(key, value)
			case "DEL":
			kvStoreEngine.Remove(key)
			default:
			log.Printf("Unknown operation in WAL: %s", operation)
		}
	})

	if err != nil {
		log.Fatalf("Failed to recover WAL data: %v", err)
	}

	kvStoreEngine.RecoveryMode = false

	// Start gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to start the server: %v", err)
	}

	grpcServer := grpc.NewServer()

	kvstoreService := &server.KVStoreService{
		Store: kvStoreEngine,
	}

	server.RegisterKvStoreServer(grpcServer, kvstoreService)

	log.Println("gRPC server started on port 50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Error while running gRPC server: %v", err)
	}
}
