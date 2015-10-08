package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/mihasya/go-metrics-librato"
	"github.com/pjvds/edgy/server"
	"github.com/rcrowley/go-metrics"
)

var (
	address      = flag.String("address", "localhost:5050", "the address to bind to")
	datadir      = flag.String("datadir", "", "the data directory")
	logmetrics   = flag.Bool("metrics", false, "turn metrics logging on or off")
	libratoEmail = flag.String("libratoemail", "", "email for librato")
	libratoToken = flag.String("libratotoken", "", "token for librato")
)

func main() {
	//defer profile.Start(profile.CPUProfile).Stop()

	flag.Parse()

	directory := *datadir
	if len(directory) == 0 {
		directory = filepath.Join(os.TempDir(), "edgy")
	}

	metrics.UseNilMetrics = true

	if *logmetrics {
		metrics.UseNilMetrics = false
		go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))
		println("console metrics enabled")
	}

	if len(*libratoEmail) > 0 && len(*libratoToken) > 0 {
		metrics.UseNilMetrics = false
		hostname, _ := os.Hostname()

		go librato.Librato(metrics.DefaultRegistry,
			5*time.Second,    // interval
			*libratoEmail,    // account owner email address
			*libratoToken,    // Librato API token
			hostname,         // source
			[]float64{0.95},  // percentiles to send
			time.Millisecond, // time unit
		)
		println("librato metrics enabled")
	}

	if err := server.ListenAndServe(*address, directory, metrics.DefaultRegistry); err != nil {
		println(err.Error())
	}
}
