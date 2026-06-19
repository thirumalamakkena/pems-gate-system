# PEMS Gate System V1

A real-time gate access validation system built using Kafka, FastAPI, MongoDB, WebSockets, and QR-based authentication.

---

## Problem Statement

In large organizations, employees enter through multiple Physical Entry Management Systems (PEMS).

The system must:

- Validate employee identity
- Check whether the employee exists
- Verify active/inactive status
- Verify gate access permissions
- Return validation results in real time
- Support multiple gate scanners simultaneously

---

## Architecture

```text
QR Code
   |
   v

Frontend Scanner
(HTML + JS)
   |
   | WebSocket
   v

Gateway API
(FastAPI)
   |
   | Kafka Producer
   v

Kafka Topic
(qr-scans)
   |
   v

Validation Consumer
(Python)
   |
   | In-Memory Cache
   v

MongoDB Users

Validation Result
   |
   v

Kafka Topic
(validation-results)
   |
   v

Gateway API
   |
   | WebSocket
   v

Frontend Scanner

ACCESS GRANTED / DENIED
```

---

## Tech Stack

### Frontend

- HTML
- CSS
- JavaScript
- html5-qrcode
- WebSockets

### Backend

- FastAPI
- Kafka
- kafka-python

### Database

- MongoDB

### Containerization

- Docker
- Docker Compose

---

## Features

### QR Authentication

Employees authenticate using QR codes.

### Real-Time Validation

Validation occurs through Kafka event streaming.

### WebSocket Communication

Validation results are pushed instantly to the scanner.

### User Cache

MongoDB data is loaded into memory for low-latency lookups.

### Multi-Gate Support

Multiple PEMS scanners can connect simultaneously.

---

## Data Flow

```text
Employee
   |
Scan QR
   |
Frontend
   |
WebSocket
   |
Gateway
   |
Kafka (qr-scans)
   |
Validation Consumer
   |
Mongo Cache Lookup
   |
Kafka (validation-results)
   |
Gateway
   |
WebSocket
   |
Frontend
```

---

## Data Contracts

### Scan Event

```json
{
  "userId": "USR001"
}
```

### Kafka Event

```json
{
  "eventId": "uuid",
  "userId": "USR001",
  "pemId": "PEMS-A",
  "timestamp": "2026-06-18T14:42:02Z"
}
```

### Validation Result

```json
{
  "eventId": "uuid",
  "userId": "USR001",
  "pemId": "PEMS-A",
  "status": "VALID"
}
```

---

## Validation Rules

### User Exists

```text
YES -> Continue
NO  -> INVALID_USER
```

### User Active

```text
ACTIVE   -> Continue
INACTIVE -> INACTIVE_USER
```

### Gate Permission

```text
Allowed     -> VALID
Not Allowed -> ACCESS_DENIED
```

---

## Project Structure

```text
PEMS-GATE-SYSTEM-V1
│
├── gateway.py
├── consumer.py
├── seed_users.py
├── generate_qr.py
├── index.html
├── docker-compose.yml
├── qrcodes/
└── README.md
```

---

## Setup

### Clone Repository

```bash
git clone <repo-url>
cd PEMS-GATE-SYSTEM-V1
```

### Install Dependencies

```bash
pip install fastapi
pip install uvicorn[standard]
pip install kafka-python
pip install pymongo
pip install qrcode
```

### Start Kafka

```bash
docker compose up -d
```

### Seed MongoDB

```bash
python seed_users.py
```

### Generate QR Codes

```bash
python generate_qr.py
```

### Start Consumer

```bash
python consumer.py
```

### Start Gateway

```bash
uvicorn gateway:app --reload
```

### Start Frontend

```bash
python -m http.server 5500
```

Open:

```text
http://localhost:5500/PEM-A
```

---

## Sample Status Codes

```text
VALID

INVALID_USER

INACTIVE_USER

ACCESS_DENIED
```

---

## Data Engineering Concepts Demonstrated

- Event Driven Architecture
- Producer Consumer Pattern
- Kafka Messaging
- Real-Time Stream Processing
- WebSockets
- Data Contracts
- Caching
- Distributed Systems Fundamentals
- Fault Tolerance Foundations

---

## Future Enhancements

### V1.1

- Display employee details
- Enhanced UI
- Access logs

### V1.2

- Multi-gate analytics
- Dashboard

### V2.0

- Apache Flink Integration
- Stateful Stream Processing
- Window Analytics
- Real-Time Monitoring

---

## Author

Thirumala , Shanmukha

Data Engineering Learning Project