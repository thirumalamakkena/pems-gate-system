# PEMS Analytics & Monitoring Platform

> A production-inspired event-driven access control platform built with **FastAPI**, **Apache Kafka**, **MongoDB Atlas**, **Redis**, **Prometheus**, and **Grafana** — fully containerized with Docker.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi)
![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-green?logo=mongodb)
![Redis](https://img.shields.io/badge/Redis-Cache-red?logo=redis)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-orange?logo=prometheus)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-orange?logo=grafana)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![Version](https://img.shields.io/badge/Version-V5-success)

---

## Overview

PEMS Analytics & Monitoring Platform is a real-time event-driven backend system that validates employee QR code scans at multiple PEMS gateways.

Instead of tightly coupling business logic into a single service, every QR scan is published as an event to Apache Kafka, allowing multiple independent consumers to process the same event simultaneously.

Version 4 added a full observability layer — every consumer and the gateway expose Prometheus metrics, and Grafana dashboards give a live view of throughput, cache performance, retries, and Kafka consumer health.

**Version 5 fully containerizes the platform.** The gateway, all 5 consumers, and the frontend now run as Docker containers alongside Kafka, Redis, Prometheus, and Grafana — built from a single shared image with per-service commands. 

The project demonstrates how modern distributed systems handle scalability, reliability, observability, and containerized deployment using asynchronous event processing.

---

## Features

### Real-Time Processing

- QR code based employee validation
- WebSocket communication
- FastAPI Gateway
- Multi-gateway support
- Event-driven processing

### Apache Kafka

- Producer / Consumer architecture
- Fan-out event processing
- Independent consumers
- Multi-partition support
- Horizontal scalability

### Reliability

- Manual offset commits
- Retry mechanism
- Retry topic
- Dead Letter Queue (DLQ)
- DLQ Replay
- Failure recovery

### Analytics

- Hourly access analytics
- Gateway-wise statistics
- Historical scan events
- Processing latency tracking

### Caching

- Redis distributed cache
- Shared cache across consumer instances
- Reduced MongoDB reads
- Improved validation latency
- Cache hit/miss monitoring

### Observability

- Prometheus instrumentation across all consumers and the gateway
- Grafana dashboards for application and Kafka health
- Consumer lag monitoring
- Batched metrics aggregation
- Service health monitoring

### Containerization (New in V5)

- Full platform runs via Docker Compose — gateway, all 5 consumers, and frontend included
- One shared image for the gateway + all consumers, different command per service

---

# Technology Stack

| Layer | Technology |
|--------|------------|
| Frontend | HTML, CSS, JavaScript |
| QR Scanner | html5-qrcode |
| Backend | FastAPI |
| Language | Python |
| Streaming | Apache Kafka (KRaft mode) |
| Database | MongoDB Atlas |
| Cache | Redis |
| Communication | WebSocket |
| Metrics | Prometheus |
| Dashboards | Grafana |
| Containerization | Docker Compose |

---

# System Architecture

> **Architecture Diagram**

![System Architecture](docs/images/system_architecture.svg)

---

# Event Lifecycle

> **Event Processing Flow**

![Event Lifecycle](docs/images/event_lifecycle.svg)

---

# Project Structure

```text
PEMS-ANALYTICS-PLATFORM
│
├── app
│   ├── api
│   │   ├── routes
│   │   └── main.py
│   │
│   ├── config
│   │   ├── database.py
│   │   ├── kafka.py
│   │   ├── redis.py
│   │   └── settings.py
│   │
│   ├── consumers
│   │   ├── validation_consumer.py
│   │   ├── analytics_consumer.py
│   │   ├── audit_consumer.py
│   │   ├── retry_consumer.py
│   │   └── dlq_consumer.py
│   │
│   ├── metrics
│   │   ├── metrics_buffer.py
│   │   └── metrics_flusher.py
│   │
│   ├── models
│   │   └── event_envelope.py
│   │
│   ├── observability
│   │   ├── prometheus.yml
│   │   └── prometheus_metrics.py
│   │
│   ├── repositories
│   │   ├── __init__.py
│   │   ├── analytics_repository.py
│   │   ├── audit_repository.py
│   │   ├── dead_letter_repository.py
│   │   ├── gateway_repository.py
│   │   ├── metrics_repository.py
│   │   └── user_repository.py
│   │
│   ├── scripts
│   │   └── replay_dlq.py
│   │
│   ├── services
│   │   ├── cache_service.py
│   │   └── validation_service.py
│   │
│   ├── simulator
│   │   └── traffic_simulator.py
│   │
│   ├── utils
│   │   ├── current_time_stamp.py
│   │   ├── event_builder.py
│   │   └── event_id_generator.py
│   │
│   └── data
│       ├── pems_gateways.json
│       └── users.json
│
├── docs
│   └── images
│       ├── system_architecture.svg
│       ├── event_lifecycle.svg
│       ├── event_envelope.svg
│       ├── kafka_topics.svg
│       ├── retry_dlq.svg
│       ├── redis_cache_architecture.svg
│       ├── metrics_pipeline.svg
│       ├── observability_architecture.svg
│       └── screenshots/
│
├── frontend
│   ├── assets
│   ├── PEM-A.html
│   └── PEM-B.html
│
├── qrcodes
│
├── .env
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── README.md
├── requirements.txt
└── test.py
```

---

## Architecture Overview

The platform follows an **event-driven architecture** where each QR scan is published as an immutable event to Apache Kafka.

Each consumer has a single responsibility:

- **Validation Consumer** validates employee access, checking Redis before falling back to MongoDB.
- **Analytics Consumer** computes hourly statistics.
- **Audit Consumer** stores immutable event history.
- **Retry Consumer** retries failed validations.
- **DLQ Consumer** stores permanently failed events.

Every consumer also exposes Prometheus metrics, scraped continuously and visualized in Grafana, so the whole pipeline is observable end-to-end rather than just log-visible.

As of V5, the gateway and all 5 consumers run as Docker containers built from a single shared image, each started with its own command — keeping deployment simple while each service still evolves independently in code.

---

# Event Envelope

Every event in the platform follows a standardized **Event Envelope** format.

This keeps communication between producers and consumers consistent while enabling tracing, retries, replay, and future schema evolution.

> **Event Envelope**

![Event Envelope](docs/images/event_envelope.svg)

### Metadata

| Field | Description |
|--------|-------------|
| eventId | Unique identifier for the event |
| version | Event schema version |
| eventType | Current stage of the event lifecycle |
| eventTime | Time when the event was created |
| producer | Service that produced the event |
| retryCount | Number of retry attempts |
| nextRetryAt | Scheduled retry timestamp |
| traceId | Tracks a request across services |
| correlationId | Groups related events |
| arrivedAt | Timestamp when consumer received the event |
| processedAt | Timestamp when processing completed |

### Payload

| Field | Description |
|--------|-------------|
| userId | Employee ID |
| pemId | PEMS Gateway ID |
| source | Event source (QR Scanner, Simulator, etc.) |
| validationStatus | Validation result |

### Sample Event

```json
{
  "metadata": {
    "eventId": "EVT-20260712163240-001",
    "version": "3.0",
    "eventType": "VALIDATION_COMPLETED",
    "eventTime": "2026-07-12T11:02:40.630Z",
    "producer": "validation-consumer",
    "retryCount": 0,
    "nextRetryAt": null,
    "traceId": "19e6a18c-9599-4922-9b17-334fd5db1297",
    "correlationId": "14c46c7a-7eaf-4a2e-a062-32454b1bd245",
    "arrivedAt": "2026-07-12T11:02:52.999Z",
    "processedAt": "2026-07-12T11:02:54.557Z"
  },
  "payload": {
    "userId": "USR036",
    "pemId": "PEMS-A",
    "source": "QR_SCANNER",
    "validationStatus": "VALID"
  }
}
```

---

# Kafka Topics

The platform uses multiple Kafka topics to decouple services and support reliable event processing.

> **Kafka Topic Architecture**

![Kafka Topics](docs/images/kafka_topics.svg)

| Topic | Producer | Consumers | Purpose |
|-------|----------|-----------|---------|
| `qr-scans` | Gateway | Validation Consumer | Incoming scan events |
| `validation-results` | Validation / Retry Consumer | Gateway, Analytics, Audit | Successful validation events |
| `retry-validation` | Validation / Retry Consumer | Retry Consumer | Failed events scheduled for retry |
| `dead-letter-validation` | Retry Consumer | DLQ Consumer | Permanently failed events |

---

# MongoDB Collections

| Collection | Purpose |
|------------|---------|
| `users` | Employee master data |
| `pems_gateways` | Gateway configuration |
| `audit_logs` | Immutable audit trail |
| `analytics_hourly` | Hourly aggregated analytics |
| `metrics` | Runtime service metrics |
| `dead_letter_events` | Permanently failed events |

---

# Reliability

The platform implements multiple reliability patterns commonly used in distributed systems.

## Retry Mechanism

- Manual Kafka offset commits
- Configurable retry attempts
- Retry delay scheduling
- Event replay support
- Retry metadata tracking

---

## Dead Letter Queue (DLQ)

Events that exceed the maximum retry attempts are moved to a Dead Letter Queue.

Instead of losing failed events, they are stored for later investigation and replay.

> **Retry & DLQ Flow**

![Retry Flow](docs/images/retry_dlq.svg)

---

## DLQ Replay

The platform supports replaying failed events after the root cause has been resolved.

Replay resets retry metadata and republishes the event back to the retry pipeline.

Benefits:

- No data loss
- Easier recovery
- Operational flexibility
- Better debugging

---

# Distributed Caching

Redis sits in front of MongoDB as a shared cache across all consumer instances, cutting validation latency and database read load.

> **Cache Flow**

![Redis Cache Architecture](docs/images/redis_cache_architecture.svg)

### Benefits

- Faster validation lookups
- Reduced MongoDB read load
- Shared cache across horizontally scaled consumers
- Cache hit/miss visibility via Prometheus

---

# Metrics Optimization

Version 3 introduced buffered metrics aggregation; Version 4 built on it with Prometheus instrumentation.

Instead of updating MongoDB for every processed event, each consumer stores metrics in memory. A background flusher periodically writes aggregated metrics to MongoDB, while the same metrics are simultaneously exposed live via Prometheus.

> **Metrics Pipeline**

![Metrics Pipeline](docs/images/metrics_pipeline.svg)

### Benefits

- Reduced database writes
- Lower MongoDB load
- Higher throughput
- Better scalability
- Live, queryable metrics in addition to persisted history

---

# Observability

Every consumer and the gateway expose a `/metrics` endpoint scraped by Prometheus, and Grafana visualizes that data as live dashboards.

> **Observability Architecture**

![Observability Architecture](docs/images/observability_architecture.svg)

## Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `events_processed_total` | Counter | Total events processed |
| `events_failed_total` | Counter | Total events that failed processing |
| `cache_hits_total` | Counter | Redis cache hits |
| `cache_misses_total` | Counter | Redis cache misses |
| `retry_events_total` | Counter | Events sent to the retry topic |
| `dlq_events_total` | Counter | Events sent to the dead letter topic |
| `validation_results_total` | Counter | VALID / INVALID validation outcomes |
| `kafka_messages_consumed_total` | Counter | Messages consumed per topic/consumer |
| `kafka_messages_produced_total` | Counter | Messages produced per topic/producer |
| `event_processing_latency_ms` | Histogram | End-to-end event processing latency |
| `active_consumers` | Gauge | Number of currently active consumer instances |

## Grafana Dashboards

**Dashboard 1 — System Overview**

![Grafana System Overview](docs/images/screenshots/grafana_system_overview.png)

**Dashboard 2 — Validation Consumer**

![Grafana Validation Consumer](docs/images/screenshots/grafana_validation_consumer.png)

**Dashboard 3 — Reliability (Retry / DLQ)**

![Grafana Reliability](docs/images/screenshots/grafana_reliability.png)

**Dashboard 4 — Kafka & Consumer Health**

![Grafana Kafka Health](docs/images/screenshots/grafana_kafka_health.png)

**Dashboard 5 — Infrastructure Monitoring** *(planned)*

---

# REST APIs

## Health

| Method | Endpoint |
|---------|----------|
| GET | `/` |

---

## Metrics

| Method | Endpoint | Description |
|---------|----------|-------------|
| GET | `/metrics` | Prometheus scrape endpoint |
| GET | `/metrics/{service}` | Get buffered metrics for a specific service |

---

## Analytics

| Method | Endpoint | Description |
|---------|----------|-------------|
| GET | `/analytics/hourly` | Hourly analytics |
| GET | `/analytics/gateway/{pemId}` | Gateway-wise analytics |

---

# Getting Started

## Prerequisites

- Docker Desktop
- Git
- A MongoDB Atlas connection string (or your own MongoDB instance)

Python 3.12+ is only needed if you want to run a service outside Docker for local debugging — not required for the standard Docker workflow below.

---

# Installation

## Clone the Repository

```bash
git clone https://github.com/thirumalamakkena/pems-gate-system/

cd pems-gate-system
```

## Configure Environment

open `.env` and confirm these match your setup:

```dotenv
MONGODB_URI="<your-mongodb-atlas-uri>"
DATABASE_NAME="pems"

KAFKA_BOOTSTRAP_SERVERS="kafka:29092"

QR_SCAN_TOPIC="qr-scans"
VALIDATION_RESULTS_TOPIC="validation-results"
RETRY_VALIDATION_TOPIC="retry-validation"
DEAD_LETTER_TOPIC="dead-letter-events"

MAX_RETRY_ATTEMPTS=3
INITIAL_RETRY_DELAY=5

REDIS_HOST="redis"
REDIS_PORT=6379
```

---

# Running the Platform (Docker)

## 1. Build Docker Images

Build all project images.

```bash
docker compose build
```

---

## 2. Start All Services

Start all containers.

```bash
docker compose up -d
```

run in detached mode (recommended).

or
```bash
docker compose up 
```

---

## 3. Verify Running Containers

Check that all services are running.

```bash
docker compose ps
```

---

## 4. View Logs

View logs from all services.

```bash
docker compose logs -f
```

View logs for a specific service.

```bash
docker compose logs -f validation-consumer
```

Example:

```bash
docker compose logs -f gateway
docker compose logs -f analytics-consumer
docker compose logs -f retry-consumer
```

---

> **Only use this step if restart need else skip**
## 5. Restart Services

Restart all services.

```bash
docker compose restart
```

Restart a specific service.

```bash
docker compose restart validation-consumer
```

---

## 6. Access the Application

| Service | URL |
|----------|-----|
| Gateway API | http://localhost:8000 |
| Frontend | http://localhost:5500/PEM-A.html |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

---
> Start testing from frontend using sample QR codes provided **qrcodes** file

---

## 7. Stop All Services

Stop all running containers.

```bash
docker compose down
```

Stop and remove associated volumes.

> **Warning:** This removes persistent Docker volumes.

```bash
docker compose down -v
```

---

## 8. Rebuild After Code Changes

If Dockerfiles or dependencies change, rebuild the images.

```bash
docker compose up --build
```

Or rebuild in detached mode.

```bash
docker compose up --build -d
```
---

# Load Testing

The project includes an event simulator for generating high-volume QR scan events.

Example configuration:

- Multiple simulated users
- Multiple gateways
- Adjustable events per second
- Configurable test duration

Typical workflow:

```
Simulator

↓

Kafka

↓

Consumers

↓

MongoDB / Redis
```

Recommended test scenarios:

| Scenario | Description |
|----------|-------------|
| Low Load | 10 events/sec |
| Medium Load | 50 events/sec |
| High Load | 100+ events/sec |
| Burst Load | Short-duration spikes |

Tracked during load tests (visible live in Grafana):

- Kafka throughput (produced / consumed per second)
- Redis cache hit vs. miss ratio under load
- End-to-end event processing latency
- Retry and DLQ volume under induced failure

---

## 🚦 Running the Traffic Simulator

> **Important**
>
> The traffic simulator is executed **outside Docker** (from your local machine).  
> Before running it, update the Kafka bootstrap server in your `.env` file.

### 1. Update `.env`

Change:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

to:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

This allows the simulator running on your host machine to connect to the Kafka broker exposed by Docker.

---

### 2. Run the Traffic Simulator

```bash
python -m app.simulator.traffic_simulator
```

---

### 3. Restore `.env`

After completing the simulation, if you plan to run the application entirely inside Docker again, change the bootstrap server back to:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

> **Note:** All Dockerized application services communicate with Kafka using the internal Docker network (`kafka:29092`), while applications running on the host machine must use `localhost:9092`.

---
# Performance Optimizations

### Event Envelope

- Standardized event format
- Better traceability
- Easier debugging

---

### Retry Pipeline

- Automatic retry
- Retry delay
- Configurable retry attempts
- Dead Letter Queue support

---

### Redis Cache

- Cache-aside pattern in front of MongoDB
- Shared across horizontally scaled consumers
- Cache hit/miss ratio tracked as a Prometheus metric

---

### Metrics Buffer

Instead of writing metrics for every event:

```
Consumer

↓

Metrics Buffer (Memory)

↓

Metrics Flusher (Every 5 Seconds)

↓

MongoDB
```

At the same time, Prometheus scrapes each service's live in-memory counters directly — no flush delay for dashboards.

Benefits:

- Reduced MongoDB writes
- Lower latency
- Improved throughput
- Live observability without extra database load

---

### Containerization (New in V5)

- Single shared Docker image for the gateway and all 5 consumers
- Container-aware Redis/Kafka configuration (no hardcoded `localhost`)

---

# Screenshots

**Frontend Gate Scanner**

![Frontend](docs/images/screenshots/frontend.png)

---

# Architecture Diagrams

The repository includes the following architecture diagrams.

| Diagram | Description |
|----------|-------------|
| `system_architecture.svg` | Complete platform architecture |
| `event_lifecycle.svg` | QR scan event lifecycle |
| `event_envelope.svg` | Standard event structure |
| `kafka_topics.svg` | Kafka topic relationships |
| `retry_dlq.svg` | Retry and DLQ workflow |
| `redis_cache_architecture.svg` | Redis cache-aside read/write flow |
| `metrics_pipeline.svg` | Buffered metrics aggregation |
| `observability_architecture.svg` | Prometheus scraping & Grafana dashboard flow |

---

# Roadmap

The project has been developed incrementally to simulate how a production-grade event-driven platform evolves over time.

## ✅ Version 1 — Foundation

- QR code scanning
- FastAPI Gateway
- Apache Kafka integration
- MongoDB persistence
- WebSocket communication
- Repository pattern

---

## ✅ Version 2 — Event-Driven Processing

- Fan-out architecture
- Validation Consumer
- Analytics Consumer
- Audit Consumer
- Hourly analytics
- Monitoring APIs
- Load testing

---

## ✅ Version 3 — Reliability & Performance

### Event Architecture

- Event Envelope
- Redis Distributed Cache
- Metadata & Payload structure
- Event versioning
- Trace ID
- Correlation ID

### Reliability

- Manual Kafka Offset Commits
- Retry Topic
- Retry Consumer
- Retry Delay
- Dead Letter Queue (DLQ)
- DLQ Consumer
- DLQ Replay

### Performance

- Metrics Buffer
- Background Metrics Flusher
- Batched MongoDB Updates
- Reduced Database Writes

---

## ✅ Version 4 — Observability

- Prometheus Integration
- Grafana Dashboards
- Consumer Lag Monitoring
- Kafka Metrics
- Custom Alerts *(in progress)*
- End-to-End Tracing *(in progress)*

---

## 🚧 Version 5 — Full Dockerization *(in progress)*

- Shared Docker image for gateway + all 5 consumers
- Container-aware Redis/Kafka configuration
- Kafka health check to gate application startup

### Planned for later in V5

- Kubernetes deployment
- CI/CD Pipeline
- Authentication (OAuth2 / JWT)
- Role-Based Access Control

---

# Learning Outcomes

This project demonstrates practical implementation of modern backend engineering and distributed systems concepts.

## Backend Engineering

- FastAPI
- Repository Pattern
- REST APIs
- WebSockets
- Service Layer Design
- Event-Driven Design

---

## Distributed Systems

- Apache Kafka
- Producer / Consumer Architecture
- Fan-Out Processing
- Retry Patterns
- Dead Letter Queue
- Event Replay
- Event Envelope
- Horizontal Scalability

---

## Data Engineering

- MongoDB Atlas
- Aggregation
- Metrics Collection
- Event Streaming
- Batch Processing
- Performance Optimization

---

## Distributed Caching

- Redis
- Cache-aside pattern
- Shared cache
- Cache hit/miss optimization

---

## Observability & Monitoring

- Prometheus instrumentation
- Grafana dashboard design
- Consumer lag tracking
- Metric types: Counters, Histograms, Gauges

---

## Containerization & Deployment

- Docker & Docker Compose
- Health checks and startup ordering

---

## Software Engineering

- Modular Architecture
- Fault Tolerance
- Reliability
- Performance Testing
- Logging
- Clean Code Practices

---

# Future Enhancements

Some ideas for extending the platform include:

- Apache Spark Streaming
- Elasticsearch
- OpenTelemetry end-to-end tracing
- Alertmanager integration
- Kubernetes
- CI/CD Pipeline
- OAuth2 / JWT Authentication
- Role-Based Access Control
- Multi-Tenant Support
- Machine Learning Based Access Prediction
- Smart Campus Dashboard
- Smart City Event Platform

---

# Contributing

Contributions, suggestions, and improvements are welcome.

If you would like to contribute:

1. Fork the repository.
2. Create a feature branch.

```bash
git checkout -b feature/my-feature
```

3. Commit your changes.

```bash
git commit -m "Add my feature"
```

4. Push to your branch.

```bash
git push origin feature/my-feature
```

5. Open a Pull Request.

---

# Author

**Thirumala**

Backend & Data Engineering Enthusiast

- Python
- Apache Kafka
- MongoDB
- Redis
- FastAPI
- Prometheus & Grafana
- Docker
- Distributed Systems
- System Design

GitHub:

```
https://github.com/thirumalamakkena
```

LinkedIn:

```
https://www.linkedin.com/in/thirumala-makkena
```

---

# Acknowledgements

This project was built as a hands-on learning platform to explore distributed systems concepts through practical implementation.

Special focus areas include:

- Event-Driven Architecture
- Apache Kafka
- MongoDB Atlas
- Redis
- FastAPI
- Reliability Patterns
- Performance Optimization
- Observability & Monitoring
- Containerization & Deployment
- System Design

---

# Architecture Assets

The README references the following diagrams and screenshots located in:

```text
docs/images/
docs/images/screenshots/
```

| File | Description |
|------|-------------|
| system_architecture.svg | Complete platform architecture |
| event_lifecycle.svg | End-to-end event flow |
| event_envelope.svg | Event metadata and payload structure |
| kafka_topics.svg | Kafka topics and consumers |
| retry_dlq.svg | Retry and Dead Letter Queue workflow |
| redis_cache_architecture.svg | Redis cache-aside flow |
| metrics_pipeline.svg | Buffered metrics aggregation |
| observability_architecture.svg | Prometheus & Grafana monitoring flow |
| screenshots/frontend.png | Gate scanner frontend |
| screenshots/grafana_*.png | Grafana dashboards |

---

# License

This project is licensed under the MIT License.

See the `LICENSE` file for more information.

---

## Repository Summary

| Category | Details |
|----------|---------|
| Architecture | Event-Driven |
| Language | Python |
| Framework | FastAPI |
| Streaming | Apache Kafka |
| Database | MongoDB Atlas |
| Cache | Redis |
| Communication | WebSocket |
| Reliability | Retry, DLQ, Replay |
| Monitoring | Prometheus, Grafana, Metrics Buffer & Batch Flusher |
| Deployment | Docker Compose |
| Scalability | Multi-Consumer Kafka Architecture |
| Version | V5 |

---

> ⭐ If you found this project useful, consider giving the repository a star!