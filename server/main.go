package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/EricHayter/yakv/proto"
	"github.com/EricHayter/yakv/server/lsm"
	"github.com/EricHayter/yakv/server/storage_manager"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type server struct {
	pb.UnimplementedYakvServerServer
	lsm *lsm.LogStructuredMergeTree
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	value, found := s.lsm.Get(in.GetKey())
	if found {
		return &pb.GetResponse{Value: value, Status: pb.Status_SUCCESS}, nil
	}
	return &pb.GetResponse{Value: "", Status: pb.Status_FAILURE}, nil
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	s.lsm.Put(in.GetKey(), in.GetValue())
	return &pb.PutResponse{Status: pb.Status_SUCCESS}, nil
}

func (s *server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.lsm.Delete(in.GetKey())
	return &pb.DeleteResponse{Status: pb.Status_SUCCESS}, nil
}

func main() {
	flag.Parse()

	// Initialize storage manager
	sm, err := storage_manager.New(100)
	if err != nil {
		slog.Error("failed to create storage manager", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Initialize LSM tree
	lsmTree, err := lsm.New(sm)
	if err != nil {
		slog.Error("failed to create LSM tree", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("shutting down gracefully...")
		if err := lsmTree.Close(); err != nil {
			slog.Error("failed to close LSM tree", slog.String("error", err.Error()))
		}
		os.Exit(0)
	}()

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		slog.Error("failed to listen", slog.String("error", err.Error()))
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterYakvServerServer(grpcServer, &server{lsm: lsmTree})
	slog.Info("server listening", slog.Any("address", lis.Addr()))

	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("failed to serve", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
