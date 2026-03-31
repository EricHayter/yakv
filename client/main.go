package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/chzyer/readline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/EricHayter/yakv/proto"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
)

const (
	welcomeString = `Welcome to YAKV (Yet Another Key Value (Store))
Type "help" for more information.`
)

func main() {
	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewYakvServerClient(conn)

	fmt.Println(welcomeString)

	rl, err := readline.New("yakv=# ")
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}

		cmd := getCommand(line)
		switch cmd {
		case "quit", "exit", "q":
			return
		case "help":
			handleHelpCommand()
		case "get", "g":
			handleGetCommand(c, line)
		case "put", "p":
			handlePutCommand(c, line)
		case "delete", "d":
			handleDeleteCommand(c, line)
		default:
			if line != "" {
				fmt.Printf("ERROR: unknown command '%s'\n", line)
			}
		}
	}
}
