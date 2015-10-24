package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/codegangsta/cli"
	"github.com/pjvds/edgy/client"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/randombytes"
	"github.com/pjvds/tidy"
)

func merge(all ...chan []byte) <-chan []byte {
	var work sync.WaitGroup
	out := make(chan []byte)

	pipe := func(in <-chan []byte) {
		for value := range in {
			out <- value
		}
		work.Done()
	}

	work.Add(len(all))
	for _, one := range all {
		go pipe(one)
	}

	go func() {
		work.Wait()
		close(out)
	}()
	return out
}

var defaultHosts = cli.StringSlice([]string{"localhost:5050"})

func main() {
	flag.Parse()
	tidy.Configure().LogFromLevelSpecifiedByEnvironment().To(tidy.Console).MustBuildDefault()

	app := cli.NewApp()
	app.Name = "edgy command line interface"
	app.Commands = []cli.Command{
		cli.Command{
			Name: "publish",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "hosts",
					Value:  "localhost:5050",
					EnvVar: "EDGY_HOSTS",
				},
				cli.StringFlag{
					Name:  "topic",
					Value: "",
				},
				cli.StringFlag{
					Name:  "key",
					Value: "",
				},
				cli.StringFlag{
					Name:  "payload",
					Value: "",
				},
			},
			Action: func(ctx *cli.Context) {
				hosts := ctx.String("hosts")
				topic := ctx.String("topic")
				key := ctx.String("key")
				payload := ctx.String("payload")

				if len(hosts) == 0 {
					fmt.Fprintf(os.Stderr, "missing hosts")
					return
				}
				if len(topic) == 0 {
					fmt.Fprintf(os.Stderr, "missing topic")
					return
				}
				if len(key) == 0 {
					fmt.Fprintf(os.Stderr, "missing partition key")
					return
				}
				if len(payload) == 0 {
					fmt.Fprintf(os.Stderr, "missing payload")
					return
				}

				if payload == "-" {
					stdin, err := ioutil.ReadAll(os.Stdin)
					if err != nil {
						fmt.Fprintf(os.Stderr, "failed to read payload from stdin")
						return
					}
					if len(stdin) == 0 {
						fmt.Fprintf(os.Stderr, "empty payload from stdin")
						return

					}
					payload = string(stdin)
				}

				builder, err := client.NewCluster().FromHosts(hosts)
				if err != nil {
					fmt.Printf("cannot build cluster: %v\n", err.Error())
					return
				}

				cluster, err := builder.Build()
				if err != nil {
					fmt.Printf("cannot build cluster: %v\n", err.Error())
					return
				}

				producer, err := client.NewProducer(cluster, client.ProducerConfig{
					QueueTime: 0,
					QueueSize: 1,
				})
				if err != nil {
					println("failed to create producer: " + err.Error())
					return
				}

				if err := producer.Append(topic, int(xxhash.Checksum32([]byte(key))), []byte(payload)).Wait(); err != nil {
					println("failed: " + err.Error())
				}
			},
		},
		cli.Command{
			Name: "writebench",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "hosts",
					Value: "localhost:5050",
				},
				cli.StringFlag{
					Name:  "topic",
					Value: "writebench",
				},
				cli.IntFlag{
					Name:  "num",
					Value: 1e6,
				},
				cli.IntFlag{
					Name:  "size",
					Value: 50,
				},
				cli.IntFlag{
					Name:  "queue.size",
					Value: 1000,
				},
				cli.DurationFlag{
					Name:  "queue.time",
					Value: 50 * time.Millisecond,
				},
			},
			Action: func(ctx *cli.Context) {
				hosts := ctx.String("hosts")
				num := ctx.Int("num")
				topic := ctx.String("topic")
				size := ctx.Int("size")
				queueSize := ctx.Int("queue.size")
				queueTime := ctx.Duration("queue.time")

				builder := client.NewCluster()

				for p, host := range strings.Split(hosts, ",") {
					builder = builder.Node(host, host, int32(p))
				}

				cluster := builder.MustBuild()

				producer, err := client.NewProducer(cluster, client.ProducerConfig{
					QueueTime: queueTime,
					QueueSize: queueSize,
				})
				if err != nil {
					println("failed to create producer: " + err.Error())
					return
				}

				work := sync.WaitGroup{}
				work.Add(num)

				started := time.Now()
				payload := randombytes.Make(size)

				for n := 0; n < num; n++ {
					result := producer.Append(topic, n, payload)

					go func(result client.AppendResult) {
						result.Wait()
						work.Done()
					}(result)
				}

				elapsed := time.Now().Sub(started)
				msgsPerSecond := float64(num) / elapsed.Seconds()
				totalMb := float64(num*size) / (1e6)

				fmt.Printf("run time: %v\n", elapsed)
				fmt.Printf("total msgs: %v\n", num)
				fmt.Printf("msgs/s: %v\n", msgsPerSecond)
				fmt.Printf("total transfered: %v\n", totalMb)
				fmt.Printf("MB/s: %v\n", totalMb/elapsed.Seconds())
				fmt.Printf("done!")
			},
		},

		cli.Command{
			Name: "consume",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "hosts",
					Value: "localhost:5050",
				},
				cli.StringFlag{
					Name:  "topics",
					Value: "writebench",
				},
				cli.BoolFlag{
					Name: "devnull",
				},
			},
			Action: func(ctx *cli.Context) {
				hosts := ctx.String("hosts")
				topics := ctx.String("topics")
				devnull := ctx.Bool("devnull")

				builder := client.NewCluster()

				for p, host := range strings.Split(hosts, ",") {
					builder = builder.Node(host, host, int32(p))
				}

				cluster := builder.MustBuild()

				consumer, err := cluster.Consume(strings.Split(topics, ",")...)
				if err != nil {
					println(err)
					return
				}

				var messageCounter, byteCounter int64
				startedAt := time.Now()

				for message := range consumer.Messages() {

					if !devnull {
						for _, rawMessage := range message.Messages.Messages() {
							value := string(rawMessage[storage.HEADER_LENGTH:])
							fmt.Fprintln(os.Stdout, value)
						}
					}

					messageCounter += int64(message.Messages.MessageCount())
					byteCounter += message.Messages.DataLen64()
				}

				elapsed := time.Since(startedAt)
				msgsPerSecond := float64(messageCounter) / elapsed.Seconds()
				totalMb := float64(byteCounter) / (1e6)

				fmt.Fprintf(os.Stderr, "run time: %v\n", elapsed)
				fmt.Fprintf(os.Stderr, "total msgs: %v\n", messageCounter)
				fmt.Fprintf(os.Stderr, "msgs/s: %f\n", msgsPerSecond)
				fmt.Fprintf(os.Stderr, "total transfered: %vmb\n", totalMb)
				fmt.Fprintf(os.Stderr, "MB/s: %v\n", totalMb/elapsed.Seconds())
				fmt.Fprintf(os.Stderr, "done!")
			},
		},
	}

	app.Run(os.Args)
}
