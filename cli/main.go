package main

import (
	"flag"

	"github.com/pjvds/edgy/client"
)

var (
	host = flag.String("host", "localhost:5050", "the edgy host address")
)

func main() {
	producer, err := client.NewProducer(*host)

	if err != nil {
		println("failed to create producer: " + err.Error())
		return
	}
	println("producer created")

	if err := producer.Append("my-topic", 1, []byte("hello world")).Wait(); err != nil {
		println("failed to append" + err.Error())
		return
	}

	println("success")
}
