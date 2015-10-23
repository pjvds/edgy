package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/codegangsta/cli"
	"github.com/mihasya/go-metrics-librato"
	"github.com/pjvds/edgy/server"
	"github.com/rcrowley/go-metrics"
)

var (
	address      = flag.String("address", ":5050", "the address to bind to")
	datadir      = flag.String("datadir", "", "the data directory")
	logmetrics   = flag.Bool("metrics", false, "turn metrics logging on or off")
	libratoEmail = flag.String("libratoemail", "", "email for librato")
	libratoToken = flag.String("libratotoken", "", "token for librato")
)

func main() {
	app := cli.NewApp()
	app.Name = "edgy host"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "address",
			Value:  ":5050",
			Usage:  "the address to bind to",
			EnvVar: "EDGY_HOST",
		},
		cli.StringFlag{
			Name:   "datadir",
			Value:  "",
			Usage:  "the data directory, if not specified it will use a temp directory",
			EnvVar: "EDGY_DATADIR",
		},
		cli.BoolFlag{
			Name:  "logmetrics",
			Usage: "turn metrics logging on",
		},
		cli.StringFlag{
			Name:  "libratoemail",
			Value: "",
			Usage: "email for librato",
		},
		cli.StringFlag{
			Name:  "libratotoken",
			Value: "",
			Usage: "token for librato",
		},
		cli.StringFlag{
			Name:   "logentries",
			Value:  "",
			Usage:  "token for logentries",
			EnvVar: "LOGENTRIES_TOKEN",
		},
	}
	app.Action = func(ctx *cli.Context) {
		address := ctx.String("address")
		directory := ctx.String("datadir")
		if len(directory) == 0 {
			directory = filepath.Join(os.TempDir(), "edgy")
		}

		metrics.UseNilMetrics = true
		if ctx.Bool("logmetrics") {
			metrics.UseNilMetrics = false
			go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))
			println("console metrics enabled")
		}

		libratoEmail := ctx.String("libratoemail")
		libratoToken := ctx.String("libratotoken")

		if len(libratoEmail) > 0 && len(libratoToken) > 0 {
			metrics.UseNilMetrics = false
			hostname, _ := os.Hostname()

			go librato.Librato(metrics.DefaultRegistry,
				5*time.Second,    // interval
				libratoEmail,     // account owner email address
				libratoToken,     // Librato API token
				hostname,         // source
				[]float64{0.95},  // percentiles to send
				time.Millisecond, // time unit
			)
			println("librato metrics enabled")
		}

		// logentriesToken := ctx.String("logentries")
		//
		// if len(logentriesToken) > 0 {
		// 	tidy.Configure().
		// 		LogFromLevel(tidy.DEBUG).
		// 		To(logentries.Configure(logentriesToken)).
		// 		MustBuildDefault()
		// }

		if err := server.ListenAndServe(address, directory, metrics.DefaultRegistry); err != nil {
			println(err.Error())
		}
	}

	app.RunAndExitOnError()
}
