package main

import (
	"distributed_kv_store/internal/kvstore"
	"distributed_kv_store/internal/server"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	kvStoreEngine := kvstore.NewKVStore()

	lis, err := net.Listen("tcp",":50051")
	if err != nil{
		log.Printf("Error: Impossible to start the server : %v", err)
	}

	grpcServer := grpc.NewServer()

	kvstoreService := &server.KVStoreService{
		Store: kvStoreEngine,
	}

	server.RegisterKvStoreServer(grpcServer,kvstoreService)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Erreur lors du lancement du serveur : %v", err)
	}

	log.Println("Serveur gRPC démarré sur le port :50051")
}