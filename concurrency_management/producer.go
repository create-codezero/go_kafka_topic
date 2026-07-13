package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "financial-transactions",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchSize:    100,   // Send 100 messages per network call
		BatchTimeout: 1 * time.Millisecond, // Or send immediately if batch fills up
	}
	defer writer.Close()

	const totalMessages = 20000
	const numWorkers = 29
	messagesPerWorker := totalMessages / numWorkers

	var wg sync.WaitGroup
	startTime := time.Now()

	// 2. Launch Workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int, startIdx, endIdx int) {
			defer wg.Done()
			
			// Each worker gets its own context to avoid one failure killing all
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			for i := startIdx; i < endIdx; i++ {
				transactionID := fmt.Sprintf("tx-amit-%05d", i)
				userID := fmt.Sprintf("user-%d", 50+i)
				amount := float64(10000+i) / 10.0

				payload := []byte(fmt.Sprintf(`{"transaction_id": "%s", "amount": %.2f, "currency": "INR", "user_id": "%s"}`, 
					transactionID, amount, userID))

				// Send in batches? 
				// For maximum speed with 2k RPS, we can send individually if network is fast,
				// but the writer's BatchSize=100 handles the batching internally if Async=false.
				// However, to force batching logic manually, we could collect 100 msgs.
				// Here we rely on the Writer's internal batching for simplicity and speed.
				
				err := writer.WriteMessages(ctx, kafka.Message{
					Key:   []byte(userID),
					Value: payload,
				})

				if err != nil {
					// Log only first few errors to avoid spam
					if i < startIdx+5 {
						log.Printf("Worker %d: Failed tx %s: %v", workerID, transactionID, err)
					}
				}
			}
		}(w, w*messagesPerWorker, (w+1)*messagesPerWorker)
	}

	wg.Wait()
	elapsed := time.Since(startTime)
	rps := float64(totalMessages) / elapsed.Seconds()

	log.Printf("✅ Completed %d messages in %.2f seconds", totalMessages, elapsed.Seconds())
	log.Printf("⚡ Throughput: %.2f messages/second", rps)
}