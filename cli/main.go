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
					partition := int32(n % cluster.Partitions())
					result := producer.Append(topic, partition, payload)

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
					Name:  "host",
					Value: "localhost:5050",
				},
				cli.StringFlag{
					Name:  "topic",
					Value: "writebench",
				},
				cli.BoolFlag{
					Name: "continuous",
				},
				cli.IntFlag{
					Name:  "partitions",
					Value: 1,
				},
			},
			Action: func(ctx *cli.Context) {
				host := ctx.String("host")
				topic := ctx.String("topic")
				continuous := ctx.Bool("continuous")
				partitions := ctx.Int("partitions")

				partitionMessages := make([]chan []byte, partitions)

				for partition := 0; partition < partitions; partition++ {
					consumer, err := client.NewConsumer(host, topic, continuous)

					if err != nil {
						println("failed to create consumer: ", err)
						return
					}

					partitionMessages[partition] = consumer.Messages
				}

				messages := merge(partitionMessages...)
				messageCounter := 0

				startedAt := time.Now()
				byteCounter := int64(0)

				for message := range messages {
					//header := storage.ReadHeader(message)
					value := string(message[storage.HEADER_LENGTH:])

					//fmt.Printf("[%v] %v\n", header.MessageId, value)

					messageCounter++
					byteCounter += int64(len(value))
				}

				elapsed := time.Since(startedAt)
				msgsPerSecond := float64(messageCounter) / elapsed.Seconds()
				totalMb := float64(byteCounter / (1e6))

				fmt.Printf("run time: %v\n", elapsed)
				fmt.Printf("total msgs: %v\n", messageCounter)
				fmt.Printf("msgs/s: %v\n", msgsPerSecond)
				fmt.Printf("total transfered: %vmb\n", totalMb)
				fmt.Printf("MB/s: %v\n", totalMb/elapsed.Seconds())
				fmt.Printf("done!")
			},
		},
	}

	app.Run(os.Args)
}
