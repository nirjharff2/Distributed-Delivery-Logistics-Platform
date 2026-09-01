package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

var dbPool *pgxpool.Pool
var redisClient *redis.Client

func initDB() {
	var err error
	connStr := "postgres://postgres:secret@user-db:5432/userdb?sslmode=disable"

	dbPool, err = pgxpool.New(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Database Connection Error: %v\n", err)
	}

	query := `
	CREATE TABLE IF NOT EXISTS users (
		id VARCHAR(50) PRIMARY KEY,
		name VARCHAR(100),
		email VARCHAR(100)
	);`
	_, err = dbPool.Exec(context.Background(), query)
	if err != nil {
		log.Fatalf("Failed to create table: %v\n", err)
	}

	seedQuery := `
	INSERT INTO users (id, name, email) 
	VALUES ('101', 'Rahim', 'rahim@example.com'), ('102', 'Karim', 'karim@example.com')
	ON CONFLICT (id) DO NOTHING;`
	dbPool.Exec(context.Background(), seedQuery)

	fmt.Println("User Service: PostgreSQL Connected!")
}

func initRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "",
		DB:       0,
	})

	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Redis Connection Error: %v\n", err)
	}

	fmt.Println("User Service: Redis Cache Connected!")
}

func getUserHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context() // Request Context bebohar kora hoyeche

	id := strings.TrimPrefix(r.URL.Path, "/users/")
	if id == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"message": "User ID is required"})
		return
	}

	cacheKey := fmt.Sprintf("user:%s", id)

	// ১. Redis Cache-এ খোঁজা
	cachedUser, err := redisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		fmt.Printf("⚡ [CACHE HIT] User %s found in Redis!\n", id)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(cachedUser))
		return
	}

	// ২. Cache MISS! PostgreSQL থেকে ডাটা আনা
	fmt.Printf("🐢 [CACHE MISS] Fetching User %s from PostgreSQL...\n", id)
	var user User
	query := "SELECT id, name, email FROM users WHERE id = $1"
	err = dbPool.QueryRow(ctx, query, id).Scan(&user.ID, &user.Name, &user.Email)

	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"message": "User not found"})
		return
	}

	// ৩. Redis Cache-এ সেভ করা
	userBytes, err := json.Marshal(user)
	if err == nil {
		err = redisClient.Set(ctx, cacheKey, userBytes, 10*time.Minute).Err()
		if err != nil {
			log.Printf("Failed to cache user in Redis: %v\n", err)
		} else {
			fmt.Printf("💾 User %s saved to Redis Cache (Valid for 10 mins)\n", id)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user)
}

func main() {
	initDB()
	defer dbPool.Close()

	initRedis()
	defer redisClient.Close()

	http.HandleFunc("/users/", getUserHandler)
	fmt.Println("User Service 8001 পোর্টে চালু হচ্ছে...")
	log.Fatal(http.ListenAndServe(":8001", nil))
}
