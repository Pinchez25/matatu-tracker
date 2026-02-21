# Matatu Tracker 🚌💨

A real-time event-driven system built with **Spring Boot** and **Apache Kafka** to track and monitor Nairobi's famous "Matatus" (public transport buses).

This project simulates a fleet of matatus broadcasting GPS coordinates and fare payments, then uses **Kafka Streams** to process these events in real-time—enriching data, generating speed alerts, and monitoring payment failures.

---

## 🚀 Features

- **Real-time GPS Tracking**: Simulates multiple matatus on different Nairobi routes (Route 33, 23, 58, 111, 46).
- **Speed Alerts**: Kafka Streams automatically filters and flags any matatu exceeding the speed threshold (default: 80 km/h).
- **Route Enrichment**: Joins location pings with static SACCO (transport company) information to provide richer data.
- **Fare Monitoring**: Processes and monitors fare payments, branching "FAILED" payments for immediate investigation.
- **Fleet Simulator**: An internal scheduler that generates realistic traffic for both locations and fares.
- **REST API**: Manually trigger events via HTTP to test specific scenarios.
- **Independent Consumer Groups**:
  - `display-board-group`: Simulates terminal display boards with live updates.
  - `location-logger-group`: Logs every movement for auditing.
  - `fare-consumer-group`: Handles final payment processing.

---

## 🛠️ Tech Stack

- **Java 17/21** (using Records for immutability)
- **Spring Boot 3.x**
- **Spring for Apache Kafka**
- **Kafka Streams**
- **Docker & Docker Compose** (for Kafka broker and UI)
- **Lombok**

---

## 🏗️ Getting Started

### 1. Prerequisites

- Docker & Docker Compose
- JDK 17+
- Maven

### 2. Start Kafka Infrastructure

Launch the Kafka broker and Kafka UI using Docker Compose:

```powershell
docker-compose up -d
```

*Kafka UI will be available at `http://localhost:8090`*

### 3. Run the Application

```powershell
./mvnw spring-boot:run
```

The simulator will start automatically, and you should see live GPS pings and fare logs in the console.

---

## 🗺️ Kafka Topology

The system uses several topics to flow data through different processing stages:

| Topic                      | Description                                          |
|:---------------------------|:-----------------------------------------------------|
| `matatu.location`          | Raw GPS pings (Input)                                |
| `matatu.fares`             | Raw fare payment events (Input)                      |
| `matatu.location.enriched` | Pings enriched with SACCO name and terminus (Output) |
| `matatu.speed.alerts`      | Filtered stream of speeding violations (Output)      |
| `matatu.fares.failed`      | Stream of only failed payment transactions (Output)  |

---

## 🕹️ Manual Control (REST API)

You can manually inject events into the system using `curl`:

### Send a Location Ping

```bash
curl -X POST http://localhost:8080/api/v1/location \
  -H "Content-Type: application/json" \
  -d '{
    "matatuId": "KDA 456B",
    "routeId": "route_33",
    "routeName": "Route 33",
    "latitude": -1.2921,
    "longitude": 36.8219,
    "speedKmh": 95.0,
    "passengersOnboard": 14
  }'
```

### Send a Fare Payment

```bash
curl -X POST http://localhost:8080/api/v1/fare \
  -H "Content-Type: application/json" \
  -d '{
    "matatuId": "KDA 456B",
    "amountKes": 70,
    "paymentMethod": "MPESA",
    "status": "FAILED"
  }'
```

---

## 📖 Key Concepts Demonstrated

- **Key-based Partitioning**: Location events for the same `routeId` always land on the same partition, guaranteeing order.
- **Stateless vs. Stateful Processing**:
  - *Stateless*: Speed filtering and failed fare branching.
  - *Stateful*: Data enrichment (using lookup maps).
- **Consumer Groups**: Multiple consumers reading the same data for different purposes without interference.
- **Kafka UI**: Use it to inspect messages, offsets, and consumer group status at `localhost:8090`.

## Spotless Formatter

This project uses [Spotless](https://github.com/diffplug/spotless) for code formatting.

```shell
# Check for formatting violations (used in CI)
./mvnw spotless:check

# Auto-fix all violations (use before committing)
./mvnw spotless:apply
```

