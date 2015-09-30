package main

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/pjvds/edgy/server"
)

var (
	address = flag.String("address", "localhost:5050", "the address to bind to")
	datadir = flag.String("datadir", "", "the data directory")
)

func main() {
	//defer profile.Start(profile.CPUProfile).Stop()

	flag.Parse()

	directory := *datadir
	if len(directory) == 0 {
		directory = filepath.Join(os.TempDir(), "edgy")
	}

	if err := server.ListenAndServe(*address, directory); err != nil {
		println(err.Error())
	}
}
