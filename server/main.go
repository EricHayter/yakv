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
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterYakvServerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
