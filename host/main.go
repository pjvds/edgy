package main

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/pjvds/edgy/server"
)

var (
	address = flag.String("address", "localhost:5050", "the address to bind to")
)

func main() {
	flag.Parse()

	directory := filepath.Join(os.TempDir(), "edgy")
	if err := server.ListenAndServe(*address, directory); err != nil {
		println(err.Error())
	}
}
