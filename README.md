# 🏆 Rinha 2025 Go - High-Performance Payment Service

## 🎯 What is Rinha de Backend?

**Rinha de Backend** is a competitive programming challenge focused on building high-performance backend systems. The 2025 edition challenges developers to create a payment processing service that maximizes profit while maintaining data consistency under extreme load conditions.

**Challenge Rules:**

- Process payments through two processors (Default: 5% fee, Fallback: 8% fee)
- Handle up to 500 concurrent users for 60 seconds
- **Penalty**: 35% profit loss for any data inconsistencies
- **Bonus**: Up to 20% profit increase for p99 latency < 10ms
- **Resource Limit**: Exactly 1.5 CPU cores + 350MB RAM

## 🚀 Key Technologies & Implementation

### **Go Backend Service**

- **Concurrency**: Leverages Go's goroutines for ultra-fast batch processing
- **HTTP Client**: Aggressive connection pooling (1000 idle connections, 500 per host)
- **Timeouts**: 800ms request timeout with 300ms response header timeout
- **Error Handling**: Intelligent fallback between payment processors

### **PostgreSQL Database**

- **Ultra-Optimized Configuration**: 250 max connections, 120MB shared buffers
- **Performance Tuning**: Disabled fsync, synchronous_commit for maximum speed
- **Batch Processing**: 10ms interval writes with 100-item buffers
- **Connection Pool**: 150 max open, 75 idle connections with aggressive lifecycle

### **Load Balancing & Infrastructure**

- **Nginx**: Round-robin load balancing between 2 API instances
- **Docker Compose**: Resource-constrained deployment (0.45 CPU, 70MB per API)
- **Health Checks**: Automatic processor failover detection

### **Concurrency & Retry Logic**

- **Batch Operations**: Asynchronous database writes every 10ms
- **Connection Reuse**: HTTP keep-alive with optimized idle timeouts
- **Processor Selection**: Health-check based routing with automatic fallback
- **No Retries**: Fail-fast approach to meet sub-10ms p99 targets

### **Performance Optimizations**

- **Ultra-Fast Cache**: 100ms summary caching under high load
- **Aggressive Timeouts**: 2s server read/write, 30s idle timeout
- **Memory Efficiency**: 16KB max header size, optimized buffer pools
- **Zero Allocation**: Pre-allocated slices and connection pools

## 📊 Current Performance Metrics

- ✅ **p99 Latency**: Targeting sub-10ms (20% profit bonus)
- ✅ **Zero Inconsistencies**: Eliminates 35% penalty
- ✅ **Success Rate**: ~58% under extreme load (8k+ requests)

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Go 1.21+

### Running the Service

```bash
# Clone the repository
git clone https://github.com/mateusgms/rinha-2025-go.git
cd rinha-2025-go

# Start the complete stack
docker-compose -f docker-compose-submission.yml up --build

# Test a payment
curl -X POST http://localhost:9999/payments \
  -H "Content-Type: application/json" \
  -d '{"correlationId":"123e4567-e89b-12d3-a456-426614174000","amount":100.50}'
```

### API Endpoints

**POST /payments** - Process payment with intelligent routing  
**GET /payments-summary** - Retrieve processing statistics

## 📈 Architecture Highlights

This implementation focuses on achieving maximum performance while maintaining data consistency, specifically optimized for the Rinha de Backend 2025 competition requirements.

---

**Built for Rinha de Backend 2025** - A competitive programming challenge focused on high-performance backend systems.
