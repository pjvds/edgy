package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/codegangsta/cli"
	"github.com/pjvds/edgy/client"
	"github.com/pjvds/edgy/storage"
)

type ReportingCounter struct {
	counter    int64
	lastCount  int64
	lastReport time.Time
}

func (this *ReportingCounter) Inc() {
	this.counter++

	now := time.Now()
	since := this.lastReport.Sub(now)

	if since.Seconds() > 1 {
		diff := float64(this.counter - this.lastCount)
		fmt.Printf("%v messages per second: %v,%v\r\n", diff/since.Seconds(), this.counter, this.lastCount)

		this.lastCount = this.counter
	}
}

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

	app := cli.NewApp()
	app.Name = "edgy command line interface"
	app.Commands = []cli.Command{
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
				cli.StringFlag{
					Name:  "payload",
					Value: "foobar",
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
				payload := []byte(ctx.String("payload"))
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

				counter := new(ReportingCounter)
				work := sync.WaitGroup{}
				work.Add(num)

				started := time.Now()

				for n := 0; n < num; n++ {
					result := producer.Append(topic, n, payload)

					go func(result client.AppendResult) {
						result.Wait()
						work.Done()
					}(result)

					counter.Inc()
				}

				elapsed := time.Now().Sub(started)
				msgsPerSecond := float64(num) / elapsed.Seconds()
				totalMb := float64(num*len(payload)) / (1e6)

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
			},
			Action: func(ctx *cli.Context) {
				hosts := ctx.String("hosts")
				topics := ctx.String("topics")

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
					value := string(message[storage.HEADER_LENGTH:])
					fmt.Fprintln(os.Stdout, value)

					messageCounter++
					byteCounter += int64(len(value))
				}

				elapsed := time.Since(startedAt)
				msgsPerSecond := float64(messageCounter) / elapsed.Seconds()
				totalMb := float64(byteCounter) / (1e6)

				fmt.Fprintf(os.Stderr, "run time: %v\n", elapsed)
				fmt.Fprintf(os.Stderr, "total msgs: %v\n", messageCounter)
				fmt.Fprintf(os.Stderr, "msgs/s: %v\n", msgsPerSecond)
				fmt.Fprintf(os.Stderr, "total transfered: %vmb\n", totalMb)
				fmt.Fprintf(os.Stderr, "MB/s: %v\n", totalMb/elapsed.Seconds())
				fmt.Fprintf(os.Stderr, "done!")
			},
		},
	}

	app.Run(os.Args)
}
