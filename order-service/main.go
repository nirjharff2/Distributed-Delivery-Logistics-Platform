package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OrderRequest struct {
	UserID string  `json:"user_id"`
	Item   string  `json:"item"`
	Amount float64 `json:"amount"`
}

type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type OrderResponse struct {
	OrderID      string  `json:"order_id"`
	CustomerName string  `json:"customer_name"`
	Item         string  `json:"item"`
	Amount       float64 `json:"amount"`
	Status       string  `json:"status"`
}

// Notification-এ পাঠানোর জন্য ইভেন্ট স্ট্রাকচার
type OrderCreatedEvent struct {
	OrderID       string  `json:"order_id"`
	CustomerName  string  `json:"customer_name"`
	CustomerEmail string  `json:"customer_email"`
	Item          string  `json:"item"`
	Amount        float64 `json:"amount"`
}

var dbPool *pgxpool.Pool
var rabbitChannel *amqp.Channel

func initDB() {
	var err error
	connStr := "postgres://postgres:secret@order-db:5432/orderdb?sslmode=disable"
	dbPool, err = pgxpool.New(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Database Connection Error: %v\n", err)
	}

	// Orders টেবিল তৈরি না থাকলে তৈরি করার কুয়েরি
	query := `
	CREATE TABLE IF NOT EXISTS orders (
		id SERIAL PRIMARY KEY,
		order_id VARCHAR(50) UNIQUE NOT NULL,
		user_id VARCHAR(50) NOT NULL,
		customer_name VARCHAR(100),
		item VARCHAR(100),
		amount NUMERIC(10, 2),
		status VARCHAR(50)
	);`

	_, err = dbPool.Exec(context.Background(), query)
	if err != nil {
		log.Fatalf("Failed to create orders table: %v\n", err)
	}

	fmt.Println("Order Service: PostgreSQL Connected & Table Ready!")
}

func initRabbitMQ() {
	// conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("RabbitMQ Connection Error: %v\n", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("RabbitMQ Channel Error: %v\n", err)
	}

	// Queue তৈরি বা চেক করা
	_, err = ch.QueueDeclare(
		"order_notifications", // Queue Name
		true,                  // Durable
		false,                 // Auto-delete
		false,                 // Exclusive
		false,                 // No-wait
		nil,
	)
	if err != nil {
		log.Fatalf("Queue Declare Error: %v\n", err)
	}

	rabbitChannel = ch
	fmt.Println("Order Service: RabbitMQ Connected!")
}

func publishNotification(event OrderCreatedEvent) {
	body, _ := json.Marshal(event)

	err := rabbitChannel.Publish(
		"",                    // Exchange
		"order_notifications", // Routing Key (Queue Name)
		false,                 // Mandatory
		false,                 // Immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		log.Printf("Failed to publish notification event: %v\n", err)
	} else {
		fmt.Println("--> RabbitMQ-তে Order Event পাঠানো হয়েছে!")
	}
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid Body", http.StatusBadRequest)
		return
	}

	// 1. User Service কল করা
	// userServiceURL := fmt.Sprintf("http://localhost:8001/users/%s", req.UserID)
	// সঠিক ফরম্যাট: URL-এর শেষে %s থাকতে হবে
	userServiceURL := fmt.Sprintf("http://user-service:8001/users/%s", req.UserID)
	resp, err := http.Get(userServiceURL)
	if err != nil || resp.StatusCode != http.StatusOK {
		http.Error(w, `{"message": "User not found"}`, http.StatusBadRequest)
		return
	}
	defer resp.Body.Close()

	var user User
	json.NewDecoder(resp.Body).Decode(&user)

	orderID := fmt.Sprintf("ORD-%d", 1000+len(req.Item))

	// 2. DB-তে সেভ করা
	insertQuery := `INSERT INTO orders (order_id, user_id, customer_name, item, amount, status) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = dbPool.Exec(context.Background(), insertQuery, orderID, req.UserID, user.Name, req.Item, req.Amount, "CONFIRMED")
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// 3. RabbitMQ-তে ইভেন্ট পাবলিশ করা (Async Notification)
	event := OrderCreatedEvent{
		OrderID:       orderID,
		CustomerName:  user.Name,
		CustomerEmail: user.Email,
		Item:          req.Item,
		Amount:        req.Amount,
	}
	go publishNotification(event)

	// Response পাঠানো
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(OrderResponse{
		OrderID:      orderID,
		CustomerName: user.Name,
		Item:         req.Item,
		Amount:       req.Amount,
		Status:       "CONFIRMED",
	})
}

func main() {
	initDB()
	defer dbPool.Close()

	initRabbitMQ()
	defer rabbitChannel.Close()

	http.HandleFunc("/orders", createOrderHandler)
	fmt.Println("Order Service 8002 পোর্টে চালু হচ্ছে...")
	log.Fatal(http.ListenAndServe(":8002", nil))
}
