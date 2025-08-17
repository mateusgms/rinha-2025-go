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

var (
	defaultProcessorHealth  = &ProcessorHealth{}
	fallbackProcessorHealth = &ProcessorHealth{}
	paymentTracker          = &PaymentTracker{}
	db                      *sql.DB

	defaultProcessorURL  = "http://payment-processor-default:8080"
	fallbackProcessorURL = "http://payment-processor-fallback:8080"

	// Ultra-aggressive HTTP client for sub-10ms p99
	httpClient = &http.Client{
		Timeout: 800 * time.Millisecond, // Ultra-fast timeout for quick failures
		Transport: &http.Transport{
			MaxIdleConns:          1000,             // Maximum connection reuse
			MaxIdleConnsPerHost:   200,              // High per-host connections
			IdleConnTimeout:       30 * time.Second, // Quick cleanup
			DisableKeepAlives:     false,
			MaxConnsPerHost:       500, // Very high concurrency
			TLSHandshakeTimeout:   500 * time.Millisecond,
			ResponseHeaderTimeout: 300 * time.Millisecond,
		},
	}

	// Batch processing for database writes
	paymentBatch []PaymentRecord
	batchMutex   sync.Mutex
	batchTimer   *time.Timer

	healthCheckInterval = 3 * time.Second
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

	// Ultra-aggressive connection pool for sub-10ms performance
	db.SetMaxOpenConns(150)                // Very high concurrency
	db.SetMaxIdleConns(75)                 // High idle connections
	db.SetConnMaxLifetime(5 * time.Minute) // Shorter lifetime for fresh connections
	db.SetConnMaxIdleTime(1 * time.Minute) // Aggressive idle cleanup

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	paymentTracker.db = db

	// Ultra-fast batch processing for sub-10ms latency
	paymentBatch = make([]PaymentRecord, 0, 100) // Much larger buffer

	// Start ultra-high-frequency batch processing
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond) // Ultra-fast: every 10ms
		defer ticker.Stop()
		for range ticker.C {
			processPendingBatch()
		}
	}()

	if err := initDatabase(); err != nil {
		log.Fatal("Failed to initialize database:", err)
	}

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
		ReadTimeout:    2 * time.Second,  // Ultra-fast response
		WriteTimeout:   2 * time.Second,  // Ultra-fast response
		IdleTimeout:    30 * time.Second, // Quick cleanup
		MaxHeaderBytes: 1 << 14,          // Reduced to 16KB for max speed
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

// Batch processing functions for better database performance
func processPendingBatch() {
	batchMutex.Lock()
	if len(paymentBatch) == 0 {
		batchMutex.Unlock()
		return
	}

	batch := make([]PaymentRecord, len(paymentBatch))
	copy(batch, paymentBatch)
	paymentBatch = paymentBatch[:0] // Clear the batch
	batchMutex.Unlock()

	if len(batch) > 0 {
		processBatch(batch)
	}
}

func processBatch(batch []PaymentRecord) {
	if len(batch) == 0 {
		return
	}

	// Use a single transaction for the entire batch
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to start transaction: %v", err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO payments (correlation_id, amount, processor, created_at) 
							VALUES ($1, $2, $3, $4) ON CONFLICT (correlation_id) DO NOTHING`)
	if err != nil {
		log.Printf("Failed to prepare batch statement: %v", err)
		return
	}
	defer stmt.Close()

	for _, payment := range batch {
		_, err := stmt.Exec(payment.CorrelationID, payment.Amount, payment.Processor, payment.Timestamp)
		if err != nil {
			log.Printf("Failed to execute batch insert: %v", err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit batch: %v", err)
	}
}

func storePaymentAsync(payment PaymentRecord) {
	batchMutex.Lock()
	paymentBatch = append(paymentBatch, payment)
	batchSize := len(paymentBatch)
	batchMutex.Unlock()

	// Process immediately if batch is full or timeout
	if batchSize >= 50 { // Larger batches for efficiency
		go processPendingBatch()
	}
}

func storePayment(payment PaymentRecord) error {
	query := `INSERT INTO payments (correlation_id, amount, processor, created_at) 
			  VALUES ($1, $2, $3, $4) ON CONFLICT (correlation_id) DO NOTHING`

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, query, payment.CorrelationID, payment.Amount, payment.Processor, payment.Timestamp)
	return err
}

func getPaymentsSummary() (*PaymentsSummary, error) {
	// Use ultra-fast cached summary (cache for 100ms under high load)
	paymentTracker.cacheMu.RLock()
	if time.Since(paymentTracker.lastCacheTime) < 100*time.Millisecond && paymentTracker.lastSummary != nil {
		summary := paymentTracker.lastSummary
		paymentTracker.cacheMu.RUnlock()
		return summary, nil
	}
	paymentTracker.cacheMu.RUnlock()

	// Query database for summary
	query := `SELECT processor, COUNT(*) as total_requests, COALESCE(SUM(amount), 0) as total_amount 
			  FROM payments 
			  GROUP BY processor`

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

		if processor == "default" {
			summary.Default.TotalRequests = totalRequests
			summary.Default.TotalAmount = totalAmount
		} else if processor == "fallback" {
			summary.Fallback.TotalRequests = totalRequests
			summary.Fallback.TotalAmount = totalAmount
		}
	}

	// Cache the result
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
	useDefault := shouldUseDefaultProcessor()

	var processorURL, processorName string
	if useDefault {
		processorURL = defaultProcessorURL
		processorName = "default"
	} else {
		processorURL = fallbackProcessorURL
		processorName = "fallback"
	}

	response, err := sendPaymentToProcessorWithRetry(processorURL, payment, 0) // No retries for speed
	if err != nil {
		// Fail fast - try alternative processor once only
		if useDefault {
			response, err = sendPaymentToProcessorWithRetry(fallbackProcessorURL, payment, 0)
			if err != nil {
				return nil, fmt.Errorf("both processors failed: %v", err)
			}
			processorName = "fallback"
		} else {
			response, err = sendPaymentToProcessorWithRetry(defaultProcessorURL, payment, 0)
			if err != nil {
				return nil, fmt.Errorf("both processors failed: %v", err)
			}
			processorName = "default"
		}
	}

	record := PaymentRecord{
		CorrelationID: payment.CorrelationID,
		Amount:        payment.Amount,
		Processor:     processorName,
		Timestamp:     time.Now(),
	}

	// Store payment in database using batch processing for better performance
	go storePaymentAsync(record)

	response.Message = "Payment processed successfully"
	return response, nil
}

func sendPaymentToProcessorWithRetry(processorURL string, payment PaymentRequest, maxRetries int) (*PaymentResponse, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*50) * time.Millisecond) // Reduced from 100ms
		}

		response, err := sendPaymentToProcessor(processorURL, payment)
		if err == nil {
			return response, nil
		}

		lastErr = err
		// Remove logging during high load to reduce overhead
		if attempt == maxRetries {
			log.Printf("Payment failed after %d attempts: %v", maxRetries+1, err)
		}
	}

	return nil, lastErr
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

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond) // Ultra-fast timeout
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
