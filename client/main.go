package main

import (
	"fmt"
	"strings"
	"github.com/chzyer/readline"
)

func is_exit_keyword(line string) bool {
	line = strings.ToLower(line)
	var exit_keywords = [...]string{"q", "quit", "exit"}
	for _, exit_keyword := range exit_keywords {
		if strings.ToLower(line) == exit_keyword {
			return true
		}
	}
	return false
}

func is_help_keyword(line string) bool {
	return strings.ToLower(line) == "help"
}

func main() {
	const welcome_string string =
`Welcome to YAKV (Yet Another Key Value (Store))
Type "help" for more information.`
	const help_string string =
`You are using yakvs, the command-line interface to yakv
Type:  "help" for help with yakv commands
       "put", or "p" to set a key value
       "get", or "g" to get a key value
       "delete", or "d" to delete a key value
       "q", "quit", "exit" to quit`

	fmt.Println(welcome_string)

	rl, err := readline.New("yakv=# ")
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil || is_exit_keyword(line) {
			break
		} else if is_help_keyword(line) {
			fmt.Println(help_string)
		} else {
			println(line)
		}
	}
}
