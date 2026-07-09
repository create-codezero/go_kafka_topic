package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// 1. Initialize the Kafka Writer (Producer configuration)
	writer := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"), // Point to your Kafka Broker
		Topic:        "financial-transactions",
		Balancer:     &kafka.LeastBytes{},        // Automatically balances load across partitions
		RequiredAcks: kafka.RequireAll,           // Ensures high data durability
	}
	defer writer.Close() // Safely close connection when the main function exits

	// 2. Define a context with a timeout for network safety
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Mock payload matching AuraStream transaction events
	payload := []byte(`{"transaction_id": "tx_98765", "amount": 5400.50, "currency": "USD"}`)

	// 3. Write message to Kafka
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("user_id_123"), // Keys keep identical user events in the same order
		Value: payload,
	})

	if err != nil {
		log.Fatalf("Could not write message to Kafka: %v", err)
	}

	log.Println("Transaction event successfully streamed to Kafka!")
}