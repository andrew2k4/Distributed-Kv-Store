package main

import (
	"distributed_kv_store/internal/kvstore"
	"distributed_kv_store/internal/pb"
	"distributed_kv_store/internal/persistence"
	"distributed_kv_store/internal/raft"
	"distributed_kv_store/internal/server"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
)

// parsePeers parses "id1=host1:port1,id2=host2:port2" into raft.Peer values.
func parsePeers(s string) ([]raft.Peer, error) {
	if s == "" {
		return nil, nil
	}
	var peers []raft.Peer
	for entry := range strings.SplitSeq(s, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer entry %q, expected id=host:port", entry)
		}
		peers = append(peers, raft.Peer{ID: parts[0], Address: parts[1]})
	}
	return peers, nil
}

func main() {
	nodeID := flag.String("id", "node1", "unique node id")
	addr := flag.String("addr", ":50051", "address this node listens on")
	peersFlag := flag.String("peers", "", "comma-separated peers, e.g. node2=host2:50051,node3=host3:50051")
	flag.Parse()

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("Invalid -peers: %v", err)
	}

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

	// Initialize Raft
	raftNode, err := raft.NewRaft(*nodeID, peers)
	if err != nil {
		log.Fatalf("Failed to start Raft: %v", err)
	}

	// Start gRPC server
	lis, err := net.Listen("tcp", *addr)
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
		Raft: raftNode,
		SnapManager: snapManager,
		Counter: counter,
	}

	pb.RegisterKvStoreServer(grpcServer, kvstoreService)

	go raftNode.Run()

	log.Printf("gRPC server started on %s (id=%s)", *addr, *nodeID)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Error while running gRPC server: %v", err)
	}
}
