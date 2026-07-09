package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// 1. Initialize the Kafka Reader (Consumer configuration)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "aurastream-processor-group", // Links multiple instances into a shared pool
		Topic:    "financial-transactions",
		MinBytes: 10e3,                         // 10KB minimum batch size
		MaxBytes: 10e6,                         // 10MB maximum batch size
	})
	defer reader.Close()

	log.Println("AuraStream Processor started. Awaiting Kafka events...")

	ctx := context.Background()

	// 2. Loop infinitely to continuously process data stream
	for {
		// ReadMessage blocks until a new message arrives in the topic
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message from stream: %v", err)
			break
		}

		// 3. Print out the received transaction details
		fmt.Printf("Processed Stream Event: Topic=%s | Partition=%d | Offset=%d | Data=%s\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
            
		// NEXT STEP: This is exactly where you will drop your Redis cache 
		// logic and database save steps!
	}
}