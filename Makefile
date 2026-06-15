.PHONY: all build clean test bench proto

# Generated files from proto
PROTO_FILES = proto/yakv.pb.go proto/yakv_grpc.pb.go

all: build

build: $(PROTO_FILES)
	go build -o ./bin/client ./client
	go build -o ./bin/server ./server

proto: $(PROTO_FILES)

$(PROTO_FILES): ./proto/yakv.proto
	protoc --go_out=. --go_opt=paths=source_relative \
	    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	    ./proto/yakv.proto

test:
	go test -race ./...

bench:
	go test -run '^$$' -bench=. -benchmem ./internal/skiplist/

clean:
	rm -rf ./bin/ $(PROTO_FILES)
