package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/pjvds/edgy/client"
	"github.com/pjvds/tidy"
)

var (
	host       = flag.String("host", "localhost:5050", "the edgy host address")
	topic      = flag.String("topic", "benchmark", "the topic to send to")
	payload    = flag.String("payload", "foobar", "the payload to send")
	partitions = flag.Int("partitions", 8, "the number of partitions")
	num        = flag.Int("num", 1*1000*1000, "")
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

	producer, err := client.NewProducer(*host)

	if err != nil {
		println("failed to create producer: " + err.Error())
		return
	}

	counter := new(ReportingCounter)
	work := sync.WaitGroup{}
	work.Add(*num)

	started := time.Now()

	for n := 0; n < *num; n++ {
		result := producer.Append(*topic, int32(n%(*partitions)), []byte(*payload))

		go func(result client.AppendResult) {
			result.Wait()
			work.Done()
		}(result)

		counter.Inc()
	}

	elapsed := time.Now().Sub(started)
	msgsPerSecond := float64(*num) / elapsed.Seconds()
	totalMb := float64(*num*len(*payload)) / (1e6)

	fmt.Printf("run time: %v\n", elapsed)
	fmt.Printf("total msgs: %v\n", *num)
	fmt.Printf("msgs/s: %v\n", msgsPerSecond)
	fmt.Printf("total transfered: %v\n", totalMb)
	fmt.Printf("MB/s: %v\n", totalMb/elapsed.Seconds())
	fmt.Printf("done!")
}
