package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
)

func main() {
	// Create a new producer
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new topic
	topic := "test_topic"

	// Create a wait group to wait for the messages to be processed
	var wg sync.WaitGroup
	wg.Add(2)

	// Producer: Send a message to Kafka
	go func() {
		defer wg.Done()

		// Create a message
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder("Hello, Kafka!"),
		}

		// Send the message
		producer.Input() <- message

		// Wait for the message to be sent
		select {
		case success := <-producer.Successes():
			fmt.Println("Produced message to topic:", success.Topic)
		case err := <-producer.Errors():
			log.Fatal("Failed to produce message:", err)
		}
	}()

	// Consumer: Consume messages from Kafka
	go func() {
		defer wg.Done()

		// Create a partition consumer
		partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
		defer partitionConsumer.Close()

		// Process messages
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				fmt.Printf("Received message from topic %s: %s\n", msg.Topic, string(msg.Value))
			case err := <-partitionConsumer.Errors():
				log.Fatal("Failed to consume message:", err)
			}
		}
	}()

	// Wait for the messages to be processed
	wg.Wait()

	// Handle shutdown gracefully
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	select {
	case <-signals:
		fmt.Println("Interrupt signal received. Shutting down...")
		producer.Close()
		consumer.Close()
	}
}
