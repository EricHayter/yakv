package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	pb "github.com/EricHayter/yakv/proto"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type server struct {
	pb.UnimplementedYakvServerServer
}

func (s *server) HandleGetRequest(ctx context.Context, in *pb.GetResponse) (*pb.GetResponse, error) {
        return &pb.GetResponse{Value: "Hi"}, nil
}

func (s *server) HandlePutRequest(ctx context.Context, in *pb.PutResponse) (*pb.PutResponse, error) {
        return &pb.PutResponse{}, nil
}

func (s *server) HandleDeleteRequest(ctx context.Context, in *pb.DeleteResponse) (*pb.DeleteResponse, error) {
        return &pb.DeleteResponse{}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterYakvServerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
