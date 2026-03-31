.PHONY: clean all

# Generated files from proto
PROTO_FILES = proto/yakv.pb.go proto/yakv_grpc.pb.go

# Source files
CLIENT_SOURCES = $(wildcard client/*.go)
SERVER_SOURCES = $(wildcard server/*.go)

all: ./bin/client ./bin/server

./bin/client: $(PROTO_FILES) $(CLIENT_SOURCES)
	go build -o ./bin/client ./client

./bin/server: $(PROTO_FILES) $(SERVER_SOURCES)
	go build -o ./bin/server ./server

$(PROTO_FILES): ./proto/yakv.proto
	protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    ./proto/yakv.proto

clean:
	rm -rf ./bin/ $(PROTO_FILES)

