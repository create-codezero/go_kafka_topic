package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
	ctx := context.Background()

	dbDSN := "host=localhost port=5432 user=postgres password=securepassword123 dbname=aurastream sslmode=disable"
	db, err := sql.Open("postgres", dbDSN)
	if err != nil {
		log.Fatalf("❌ Failed to initialize PostgreSQL driver: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("❌ Database ping failed: %v", err)
	}
	log.Println("✅ Connected to PostgreSQL")

	
	if err := ensureTableExists(db); err != nil {
		log.Fatalf("❌ Failed to setup database table: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("⚠️ Redis connection failed (non-critical): %v", err)
	} else {
		log.Println("✅ Connected to Redis")
	}

	// 3. Setup Kafka Reader
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "financial-transactions",
		GroupID:        "aurastream-group-v3",
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        10 * time.Millisecond,
		CommitInterval: time.Second,
	})
	defer kafkaReader.Close()

	log.Println("🚀 Kafka consumer started. Waiting for messages...")

	for {
		msg, err := kafkaReader.ReadMessage(ctx)
		if err == nil {
			log.Printf("📥 RAW KAFKA BYTES RECEIVED: %s", string(msg.Value))
		}
		if err != nil {
			log.Printf("⚠️ Error reading from Kafka: %v. Retrying...", err)
			time.Sleep(2 * time.Second)
			continue
		}

		var txn Transaction
		if err := json.Unmarshal(msg.Value, &txn); err != nil {
			log.Printf("❌ Failed to unmarshal JSON: %v. Message: %s", err, string(msg.Value))
			continue
		}

		log.Printf("📥 Received: %s - $%.2f %s", txn.TransactionID, txn.Amount, txn.Currency)

		if err := insertTransaction(db, txn); err != nil {
			log.Printf("❌ Failed to insert into DB: %v. Attempting to cache in Redis...", err)

			cacheKey := fmt.Sprintf("failed_txn:%s", txn.TransactionID)
			jsonData, _ := json.Marshal(txn)
			if err := rdb.Set(ctx, cacheKey, jsonData, 24*time.Hour).Err(); err != nil {
				log.Printf("❌ Also failed to cache in Redis: %v", err)
			} else {
				log.Printf("💾 Cached failed transaction in Redis: %s", cacheKey)
			}
			continue
		}

		log.Printf("✅ Transaction %s successfully committed to DB", txn.TransactionID)
	}
}

func ensureTableExists(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS transactions (
		transaction_id VARCHAR(50) PRIMARY KEY,
		user_id VARCHAR(50) NOT NULL,
		amount NUMERIC(10, 2) NOT NULL,
		currency VARCHAR(10) NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := db.Exec(schema)
	return err
}

func insertTransaction(db *sql.DB, txn Transaction) error {
	query := `
		INSERT INTO transactions (transaction_id, user_id, amount, currency)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (transaction_id) DO NOTHING;
	`
	_, err := db.Exec(
		query,
		txn.TransactionID,
		txn.UserID,
		txn.Amount,
		txn.Currency,
	)
	return err
}