package main

import (
	"distributed_kv_store/internal/kvstore"
	"distributed_kv_store/internal/persistence"
	"distributed_kv_store/internal/server"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
)

func main() {
	walPath := "wal.log"
	snapshotPath := "snapshot.json"

	// Initialize WAL
	walEngine, err := persistence.NewWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to start WAL: %v", err)
	}

	// Initialize KV store
	kvStoreEngine := kvstore.NewKVStore(walEngine)

	// Enable recovery mode to avoid rewriting WAL during recovery
	kvStoreEngine.RecoveryMode = true

	// Load snapshot if it exists
	snapshotFile, err := os.OpenFile(snapshotPath, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open snapshot file: %v", err)
	}
	defer snapshotFile.Close()

	var data map[string]string
	if err := json.NewDecoder(snapshotFile).Decode(&data); err != nil && err != io.EOF {
		log.Fatalf("Failed to decode snapshot data: %v", err)
	}
	for key, value := range data {
		kvStoreEngine.Set(key, value)
	}

	
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
	// Initialize Snapshot Manager
	snapManager := &server.SnapshotManager{}
	// Create Counter for operations
	counter := &server.OperationCounter{}	
	kvstoreService := &server.KVStoreService{
		Store: kvStoreEngine,
		SnapManager: snapManager,
		Counter: counter,
	}
 
	server.RegisterKvStoreServer(grpcServer, kvstoreService)

	log.Println("gRPC server started on port 50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Error while running gRPC server: %v", err)
	}
}
