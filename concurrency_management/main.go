package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	UserID        string  `json:"user_id"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initializations (Keep your existing Postgres & Redis setup)
	dbDSN := "host=localhost port=5432 user=postgres password=securepassword123 dbname=aurastream sslmode=disable"
	db, err := sql.Open("postgres", dbDSN)
	if err != nil { log.Fatalf("❌ Postgres Init Error: %v", err) }
	defer db.Close()
	
	// Maximize database connection pool for concurrent workers
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "financial-transactions",
		GroupID: "aurastream-group-v4",
	})
	defer kafkaReader.Close()

	// 2. Setup the Internal Pipe (Buffered Channel)
	// Holds up to 10,000 un-parsed Kafka messages in memory
	jobChannel := make(chan kafka.Message, 10000)

	// 3. Launch the Worker Pool
	numWorkers := 29
	var wg sync.WaitGroup

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		// Fire up independent worker threads
		go worker(ctx, i, jobChannel, db, rdb, &wg)
	}
	log.Printf("⚙️ Spawned %d background database processors", numWorkers)

	// 4. Main Thread: Dedicated Kafka Reader Loop
	log.Println("🚀 Fast-ingestion loop active. Streaming events...")
	for {
		msg, err := kafkaReader.ReadMessage(ctx)
		if err != nil {
			log.Printf("⚠️ Kafka read error: %v", err)
			continue
		}

		// Ship message straight to the channel buffer without waiting for DB execution
		jobChannel <- msg
	}
}

// Dedicated background worker function
func worker(ctx context.Context, id int, jobs <-chan kafka.Message, db *sql.DB, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	for msg := range jobs {
		var txn Transaction
		if err := json.Unmarshal(msg.Value, &txn); err != nil {
			log.Printf("[Worker %d] ❌ JSON Unmarshal Failed", id)
			continue
		}

		// Database persistent write
		if err := insertTransaction(db, txn); err != nil {
			log.Printf("[Worker %d] ❌ DB Fail -> Caching to Redis", id)
			cacheKey := fmt.Sprintf("failed_txn:%s", txn.TransactionID)
			rdb.Set(ctx, cacheKey, msg.Value, 24*time.Hour)
			continue
		}

		log.Printf("[Worker %d] ✅ Processed Txn: %s", id, txn.TransactionID)
	}
}

func insertTransaction(db *sql.DB, txn Transaction) error {
	query := `INSERT INTO transactions (transaction_id, user_id, amount, currency)
	          VALUES ($1, $2, $3, $4) ON CONFLICT (transaction_id) DO NOTHING;`
	_, err := db.Exec(query, txn.TransactionID, txn.UserID, txn.Amount, txn.Currency)
	return err
}