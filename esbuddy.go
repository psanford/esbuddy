package main

import (
	"fmt"
	"log"
	"os"

	"github.com/psanford/esbuddy/cmd"
)

func main() {
	log.SetFlags(0)

	err := cmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
