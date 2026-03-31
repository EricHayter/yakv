package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "github.com/EricHayter/yakv/proto"
)

func getCommand(line string) string {
	command, _, _ := strings.Cut(strings.ToLower(line), " ")
	return command
}

// handleHelpCommand displays help information
func handleHelpCommand() {
	const helpString = `You are using yakvs, the command-line interface to yakv
Type:  "help" for help with yakv commands
       "put", or "p" to set a key value
       "get", or "g" to get a key value
       "delete", or "d" to delete a key value
       "q", "quit", "exit" to quit`
	fmt.Println(helpString)
}

// handlePutCommand handles the put command: put key value
func handlePutCommand(c pb.YakvServerClient, line string) {
	words := strings.Fields(line)
	if len(words) != 3 {
		fmt.Println("ERROR: put command requires key and value (usage: put <key> <value>)")
		return
	}

	key := words[1]
	value := words[2]

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		fmt.Printf("ERROR: could not put: %v\n", err)
		return
	}
	if r.GetStatus() == pb.Status_SUCCESS {
		fmt.Println("OK")
	} else {
		fmt.Println("FAILED")
	}
}

// handleGetCommand handles the get command: get key
func handleGetCommand(c pb.YakvServerClient, line string) {
	words := strings.Fields(line)
	if len(words) != 2 {
		fmt.Println("ERROR: get command requires key (usage: get <key>)")
		return
	}

	key := words[1]

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		fmt.Printf("ERROR: could not get: %v\n", err)
		return
	}
	if r.GetStatus() == pb.Status_SUCCESS {
		fmt.Printf("%s = %s\n", key, r.GetValue())
	} else {
		fmt.Printf("FAILED: key '%s' not found\n", key)
	}
}

// handleDeleteCommand handles the delete command: delete key
func handleDeleteCommand(c pb.YakvServerClient, line string) {
	words := strings.Fields(line)
	if len(words) != 2 {
		fmt.Println("ERROR: delete command requires key (usage: delete <key>)")
		return
	}

	key := words[1]

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		fmt.Printf("ERROR: could not delete: %v\n", err)
		return
	}
	if r.GetStatus() == pb.Status_SUCCESS {
		fmt.Println("OK")
	} else {
		fmt.Println("FAILED")
	}
}
