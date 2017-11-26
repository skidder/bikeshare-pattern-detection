package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/skidder/gbfs-go/gbfs"
	"github.com/streadway/amqp"
)

var (
	feedURL           = flag.String("feed", "https://gbfs.fordgobike.com/gbfs/gbfs.json", "GBFS feed auto-discovery URL")
	rabbitmqURI       = flag.String("rabbitmq", "amqp://guest:guest@rabbitmq:5672/", "RabbitMQ connection URI")
	rabbitmqQueueName = flag.String("queue", "gbfs-feed", "RabbitMQ queue name")
	startupDelay      = flag.Int64("delay_seconds", 30, "Seconds to delay connection to RabbitMQ on startup")
	pollingInterval   = flag.Int64("polling_seconds", 60, "Seconds between polling the feed")
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	flag.Parse()
	time.Sleep(time.Second * time.Duration(*startupDelay))

	conn, err := amqp.Dial(*rabbitmqURI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	for {
		client := gbfs.NewClient(*feedURL)
		stationInfo, err := client.GetStationStatus("en")
		if err != nil {
			fmt.Printf("Error while retrieving data: %s", err.Error())
			return
		}

		q, err := ch.QueueDeclare(
			*rabbitmqQueueName, // name
			false,              // durable
			false,              // delete when unused
			false,              // exclusive
			false,              // no-wait
			nil,                // arguments
		)
		failOnError(err, "Failed to declare a queue")

		encodedBytes, err := proto.Marshal(stationInfo)
		if err != nil {
			fmt.Printf("Error while marshalling data to Proto: %v", err.Error())
			return
		}
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/x-protobuf",
				Body:        encodedBytes,
			})
		failOnError(err, "Failed to publish a message")

		fmt.Printf("Successfuly sent message to RabbitMQ, sleeping %d seconds\n", *pollingInterval)
		time.Sleep(time.Second * time.Duration(*pollingInterval))
	}
}
