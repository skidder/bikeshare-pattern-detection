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
	rabbitmqQueueName = flag.String("queue", "gbfs-complete-feed", "RabbitMQ queue name")
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

	client := gbfs.NewClient(*feedURL)
	q, err := ch.QueueDeclare(
		*rabbitmqQueueName, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for {
		gbfsData, err := client.GetCompleteGBFSMessage("en")
		if err != nil {
			log.Printf("Error while retrieving data: %s", err.Error())
		}

		encodedBytes, err := proto.Marshal(gbfsData)
		if err != nil {
			log.Printf("Error while marshalling data to Proto: %v", err.Error())
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
		if err != nil {
			log.Printf("Failed to publish message: %s", err.Error())
			continue
		}

		log.Printf("Successfully sent message to RabbitMQ, sleeping %d seconds\n", *pollingInterval)
		time.Sleep(time.Second * time.Duration(*pollingInterval))
	}
}
