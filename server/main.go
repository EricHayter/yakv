package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	pb "github.com/EricHayter/yakv/proto"
)

var (
	port = flag.Int("port", 50051, "The server port")
	database = make(map[string]string)
)

type server struct {
	pb.UnimplementedYakvServerServer
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	value, prs := database[in.GetKey()]
	if prs {
		return &pb.GetResponse{Value: value, Status: pb.Status_SUCCESS}, nil
	} else {
		return &pb.GetResponse{Value: value, Status: pb.Status_FAILURE}, nil
	}
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	database[in.GetKey()] = in.GetValue()
	return &pb.PutResponse{Status: pb.Status_SUCCESS}, nil
}

func (s *server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	delete(database, in.GetKey())
	return &pb.DeleteResponse{ Status: pb.Status_SUCCESS}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		slog.Error("failed to listen", slog.String("error", err.Error()))
	}
	s := grpc.NewServer()
	pb.RegisterYakvServerServer(s, &server{})
	slog.Info("server listening", slog.Any("address", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		slog.Error("failed to serve", slog.String("error", err.Error()))
	}
}
