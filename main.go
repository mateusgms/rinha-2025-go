package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

type PaymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

type PaymentResponse struct {
	Message string `json:"message,omitempty"`
}

type ProcessorHealthResponse struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

type PaymentsSummary struct {
	Default  ProcessorStats `json:"default"`
	Fallback ProcessorStats `json:"fallback"`
}

type ProcessorStats struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type ProcessorHealth struct {
	Failing         bool
	LastChecked     time.Time
	MinResponseTime int
	mu              sync.RWMutex
}

type PaymentRecord struct {
	CorrelationID string
	Amount        float64
	Processor     string
	Timestamp     time.Time
}

type PaymentTracker struct {
	db            *sql.DB
	mu            sync.RWMutex
	lastSummary   *PaymentsSummary
	lastCacheTime time.Time
	cacheMu       sync.RWMutex
}

// Add a deduplication cache to prevent race conditions
type DeduplicationCache struct {
	processedPayments map[string]bool
	mu                sync.RWMutex
}

var (
	defaultProcessorHealth  = &ProcessorHealth{}
	fallbackProcessorHealth = &ProcessorHealth{}
	paymentTracker          = &PaymentTracker{}
	deduplicationCache      = &DeduplicationCache{processedPayments: make(map[string]bool)}
	db                      *sql.DB

	defaultProcessorURL  = "http://payment-processor-default:8080"
	fallbackProcessorURL = "http://payment-processor-fallback:8080"

	// Ultra-fast HTTP client optimized for sub-10ms p99
	httpClient = &http.Client{
		Timeout: 300 * time.Millisecond, // More conservative timeout to reduce failures
		Transport: &http.Transport{
			MaxIdleConns:          200,              // Reduced to prevent memory issues
			MaxIdleConnsPerHost:   30,               // More focused connections
			IdleConnTimeout:       10 * time.Second, // Longer to reuse connections
			DisableKeepAlives:     false,
			MaxConnsPerHost:       50, // More conservative concurrency
			TLSHandshakeTimeout:   100 * time.Millisecond,
			ResponseHeaderTimeout: 100 * time.Millisecond,
			DisableCompression:    true, // Reduce CPU overhead
			WriteBufferSize:       4096, // Small buffer for speed
			ReadBufferSize:        4096, // Small buffer for speed
		},
	}

	healthCheckInterval = 5 * time.Second
)

func main() {
	port := os.Getenv("APP_PORT")
	if port == "" {
		port = "8080"
	}

	if url := os.Getenv("DEFAULT_PROCESSOR_URL"); url != "" {
		defaultProcessorURL = url
	}

	if url := os.Getenv("FALLBACK_PROCESSOR_URL"); url != "" {
		fallbackProcessorURL = url
	}

	// Initialize PostgreSQL connection with optimized settings
	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		postgresURL = "postgres://user:password@postgres:5432/dbname?sslmode=disable"
	}

	var err error
	db, err = sql.Open("postgres", postgresURL)
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Optimized connection pool for speed and consistency
	db.SetMaxOpenConns(30)                  // Further reduced to prevent lock contention
	db.SetMaxIdleConns(15)                  // Fewer idle connections
	db.SetConnMaxLifetime(90 * time.Second) // Shorter lifetime to prevent stale connections
	db.SetConnMaxIdleTime(30 * time.Second) // Faster cleanup

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	paymentTracker.db = db

	if err := initDatabase(); err != nil {
		log.Fatal("Failed to initialize database:", err)
	}

	// Start cache cleanup routine to prevent memory bloat
	go func() {
		ticker := time.NewTicker(10 * time.Second) // More frequent cleanup
		defer ticker.Stop()
		for range ticker.C {
			deduplicationCache.mu.Lock()
			// Clear cache more aggressively to prevent memory bloat and race conditions
			if len(deduplicationCache.processedPayments) > 5000 {
				deduplicationCache.processedPayments = make(map[string]bool)
				log.Printf("Cleared deduplication cache (had %d entries)", len(deduplicationCache.processedPayments))
			}
			deduplicationCache.mu.Unlock()
		}
	}()

	r := mux.NewRouter()
	r.HandleFunc("/payments", handlePayments).Methods("POST")
	r.HandleFunc("/payments-summary", handlePaymentsSummary).Methods("GET")

	log.Printf("Server starting on port %s", port)
	log.Printf("Default processor: %s", defaultProcessorURL)
	log.Printf("Fallback processor: %s", fallbackProcessorURL)
	log.Printf("Database connected: %s", postgresURL)

	server := &http.Server{
		Addr:           ":" + port,
		Handler:        r,
		ReadTimeout:    1 * time.Second,  // Ultra-fast response
		WriteTimeout:   1 * time.Second,  // Ultra-fast response
		IdleTimeout:    15 * time.Second, // Quick cleanup
		MaxHeaderBytes: 1 << 12,          // 4KB for maximum speed
	}

	log.Fatal(server.ListenAndServe())
}

func initDatabase() error {
	// Create payments table with optimized indexes for PostgreSQL
	query := `
	CREATE TABLE IF NOT EXISTS payments (
		id SERIAL PRIMARY KEY,
		correlation_id VARCHAR(36) NOT NULL UNIQUE,
		amount DECIMAL(10,2) NOT NULL,
		processor VARCHAR(20) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	_, err := db.Exec(query)
	if err != nil {
		log.Printf("Warning: Failed to create payments table: %v", err)
		// Continue anyway, table might already exist
	}

	// Create optimized indexes (ignore errors if already exist)
	indexQueries := []string{
		"CREATE INDEX IF NOT EXISTS idx_payments_processor ON payments(processor)",
		"CREATE INDEX IF NOT EXISTS idx_payments_created_at ON payments(created_at)",
		"CREATE INDEX IF NOT EXISTS idx_payments_correlation_id ON payments(correlation_id)",
	}

	for _, indexQuery := range indexQueries {
		if _, err := db.Exec(indexQuery); err != nil {
			log.Printf("Warning: Failed to create index: %v", err)
		}
	}

	log.Println("Database initialized successfully")
	return nil
}

func getPaymentsSummary() (*PaymentsSummary, error) {
	// Use cache for only 10ms to ensure more accurate results under high load
	paymentTracker.cacheMu.RLock()
	if time.Since(paymentTracker.lastCacheTime) < 10*time.Millisecond && paymentTracker.lastSummary != nil {
		summary := paymentTracker.lastSummary
		paymentTracker.cacheMu.RUnlock()
		return summary, nil
	}
	paymentTracker.cacheMu.RUnlock()

	// Query database for summary with optimized timeout
	query := `SELECT processor, COUNT(*) as total_requests, COALESCE(SUM(amount), 0) as total_amount 
			  FROM payments 
			  GROUP BY processor`

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second) // Longer timeout for reliability
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	summary := &PaymentsSummary{
		Default:  ProcessorStats{TotalRequests: 0, TotalAmount: 0},
		Fallback: ProcessorStats{TotalRequests: 0, TotalAmount: 0},
	}

	for rows.Next() {
		var processor string
		var totalRequests int
		var totalAmount float64

		if err := rows.Scan(&processor, &totalRequests, &totalAmount); err != nil {
			return nil, err
		}

		switch processor {
		case "default":
			summary.Default.TotalRequests = totalRequests
			summary.Default.TotalAmount = totalAmount
		case "fallback":
			summary.Fallback.TotalRequests = totalRequests
			summary.Fallback.TotalAmount = totalAmount
		}
	}

	// Cache the result for fast access
	paymentTracker.cacheMu.Lock()
	paymentTracker.lastSummary = summary
	paymentTracker.lastCacheTime = time.Now()
	paymentTracker.cacheMu.Unlock()

	return summary, nil
}

func handlePayments(w http.ResponseWriter, r *http.Request) {
	var payment PaymentRequest

	if err := json.NewDecoder(r.Body).Decode(&payment); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid JSON"})
		return
	}

	if payment.CorrelationID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "correlationId is required"})
		return
	}

	payment.CorrelationID = strings.TrimSpace(payment.CorrelationID)

	if _, err := uuid.Parse(payment.CorrelationID); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "correlationId must be a valid UUID"})
		return
	}

	if payment.Amount <= 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "amount must be a positive number"})
		return
	}

	response, err := processPayment(payment)
	if err != nil {
		log.Printf("Payment processing failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Payment processing failed"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func processPayment(payment PaymentRequest) (*PaymentResponse, error) {
	// Check for duplicate processing at application level first (fastest check)
	deduplicationCache.mu.Lock()
	if deduplicationCache.processedPayments[payment.CorrelationID] {
		deduplicationCache.mu.Unlock()
		return &PaymentResponse{Message: "Payment already processed"}, nil
	}
	// Mark as being processed immediately to prevent race conditions
	deduplicationCache.processedPayments[payment.CorrelationID] = true
	deduplicationCache.mu.Unlock()

	// Use a transaction to atomically check and insert to prevent race conditions
	tx, err := db.Begin()
	if err != nil {
		deduplicationCache.mu.Lock()
		delete(deduplicationCache.processedPayments, payment.CorrelationID)
		deduplicationCache.mu.Unlock()
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback() // Will be no-op if we commit

	// Check if payment already exists within the transaction
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var exists bool
	checkQuery := `SELECT EXISTS(SELECT 1 FROM payments WHERE correlation_id = $1 FOR UPDATE)`
	err = tx.QueryRowContext(ctx, checkQuery, payment.CorrelationID).Scan(&exists)
	if err != nil {
		deduplicationCache.mu.Lock()
		delete(deduplicationCache.processedPayments, payment.CorrelationID)
		deduplicationCache.mu.Unlock()
		return nil, fmt.Errorf("failed to check payment existence: %v", err)
	}

	if exists {
		tx.Rollback()
		return &PaymentResponse{Message: "Payment already processed"}, nil
	}

	useDefault := shouldUseDefaultProcessor()

	var processorName string
	var response *PaymentResponse

	// Try primary processor first
	if useDefault {
		processorName = "default"
		response, err = sendPaymentToProcessor(defaultProcessorURL, payment)

		// If primary fails, try fallback ONCE
		if err != nil {
			processorName = "fallback"
			response, err = sendPaymentToProcessor(fallbackProcessorURL, payment)
		}
	} else {
		processorName = "fallback"
		response, err = sendPaymentToProcessor(fallbackProcessorURL, payment)

		// If primary fails, try fallback ONCE
		if err != nil {
			processorName = "default"
			response, err = sendPaymentToProcessor(defaultProcessorURL, payment)
		}
	}

	// If both processors failed, rollback and fail
	if err != nil {
		tx.Rollback()
		deduplicationCache.mu.Lock()
		delete(deduplicationCache.processedPayments, payment.CorrelationID)
		deduplicationCache.mu.Unlock()
		return nil, fmt.Errorf("both processors failed: %v", err)
	}

	// Insert payment within transaction to ensure atomicity
	query := `INSERT INTO payments (correlation_id, amount, processor, created_at) 
			  VALUES ($1, $2, $3, $4)`

	_, dbErr := tx.ExecContext(ctx, query, payment.CorrelationID, payment.Amount, processorName, time.Now())
	if dbErr != nil {
		tx.Rollback()
		deduplicationCache.mu.Lock()
		delete(deduplicationCache.processedPayments, payment.CorrelationID)
		deduplicationCache.mu.Unlock()
		return nil, fmt.Errorf("failed to store payment: %v", dbErr)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		deduplicationCache.mu.Lock()
		delete(deduplicationCache.processedPayments, payment.CorrelationID)
		deduplicationCache.mu.Unlock()
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	response.Message = "Payment processed successfully"
	return response, nil
}

func sendPaymentToProcessor(processorURL string, payment PaymentRequest) (*PaymentResponse, error) {
	requestData := map[string]interface{}{
		"correlationId": payment.CorrelationID,
		"amount":        payment.Amount,
		"requestedAt":   time.Now().UTC().Format(time.RFC3339),
	}

	paymentJSON, err := json.Marshal(requestData)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond) // Match client timeout
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", processorURL+"/payments", bytes.NewBuffer(paymentJSON))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("processor returned status %d", resp.StatusCode)
	}

	return &PaymentResponse{}, nil
}

func shouldUseDefaultProcessor() bool {
	defaultProcessorHealth.mu.RLock()
	defaultLastChecked := defaultProcessorHealth.LastChecked
	defaultFailing := defaultProcessorHealth.Failing
	defaultProcessorHealth.mu.RUnlock()

	fallbackProcessorHealth.mu.RLock()
	fallbackLastChecked := fallbackProcessorHealth.LastChecked
	fallbackFailing := fallbackProcessorHealth.Failing
	fallbackProcessorHealth.mu.RUnlock()

	if time.Since(defaultLastChecked) >= healthCheckInterval {
		go checkProcessorHealth(defaultProcessorURL, defaultProcessorHealth)
	}

	if time.Since(fallbackLastChecked) >= healthCheckInterval {
		go checkProcessorHealth(fallbackProcessorURL, fallbackProcessorHealth)
	}

	if defaultLastChecked.IsZero() {
		return true
	}

	if fallbackLastChecked.IsZero() {
		return true
	}

	if !defaultFailing {
		return true
	}

	if !fallbackFailing {
		return false
	}

	return true
}

func checkProcessorHealth(processorURL string, health *ProcessorHealth) {
	health.mu.Lock()
	defer health.mu.Unlock()

	if time.Since(health.LastChecked) < healthCheckInterval {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", processorURL+"/payments/service-health", nil)
	if err != nil {
		health.Failing = true
		health.LastChecked = time.Now()
		return
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		health.Failing = true
		health.LastChecked = time.Now()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return
	}

	if resp.StatusCode != 200 {
		health.Failing = true
		health.LastChecked = time.Now()
		return
	}

	var healthResp ProcessorHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&healthResp); err != nil {
		health.Failing = true
		health.LastChecked = time.Now()
		return
	}

	health.Failing = healthResp.Failing
	health.MinResponseTime = healthResp.MinResponseTime
	health.LastChecked = time.Now()
}

func handlePaymentsSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := getPaymentsSummary()
	if err != nil {
		log.Printf("Failed to get payments summary: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to retrieve payments summary"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(summary)
}
