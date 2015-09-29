package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/codegangsta/cli"
	"github.com/pjvds/edgy/client"
	"github.com/pjvds/tidy"
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

func main() {
	tidy.Configure().LogFromLevel(tidy.ERROR).To(tidy.Console).BuildDefault()
	flag.Parse()

	app := cli.NewApp()
	app.Name = "edgy command line interface"
	app.Commands = []cli.Command{
		cli.Command{
			Name: "writebench",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "host",
					Value: "localhost:5050",
				},
				cli.StringFlag{
					Name:  "topic",
					Value: "writebench",
				},
				cli.IntFlag{
					Name:  "partitions",
					Value: 1,
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
				host := ctx.String("host")
				num := ctx.Int("num")
				topic := ctx.String("topic")
				partitions := ctx.Int("partitions")
				payload := []byte(ctx.String("payload"))
				queueSize := ctx.Int("queue.size")
				queueTime := ctx.Duration("queue.time")

				producer, err := client.NewProducer(host, client.ProducerConfig{
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
					partition := int32(n % partitions)
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
	}

	app.Run(os.Args)
}
