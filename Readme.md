# Real-Time Payment Fraud Detection System

A real-time payment fraud detection application built using Java, Spring Boot, Apache Kafka, Kafka Streams, PostgreSQL, and Google Gemini AI.

The system processes payment transactions as streaming events, evaluates them against configurable fraud detection rules, categorizes transactions, persists the results, and provides a web dashboard for monitoring and AI-assisted fraud explanations.

## Project Overview

Financial applications process a large number of payment transactions. Identifying suspicious transactions quickly is important for reducing potential fraud.

This project demonstrates an event-driven fraud detection workflow using Apache Kafka and Kafka Streams, combined with an AI-powered explanation feature using Google Gemini.

When a payment transaction is submitted:

1. Spring Boot exposes a REST API to receive the transaction.
2. The transaction is published to an Apache Kafka topic.
3. Kafka Streams processes and evaluates the transaction.
4. Fraud detection rules determine the transaction status.
5. The result is routed to the appropriate Kafka topic.
6. Transaction details and evaluation results are saved in PostgreSQL.
7. A web dashboard displays transaction statistics and recent transactions.
8. Google Gemini generates an explanation of the fraud evaluation when requested.

## Architecture

```text
                 Payment Client
                       |
                       v
              Spring Boot REST API
                       |
                       v
                Kafka Producer
                       |
                       v
              transactions-topic
                       |
                       v
                 Kafka Streams
                       |
                       v
             Fraud Detection Engine
                       |
             +---------+---------+
             |         |         |
             v         v         v
          APPROVED   REVIEW    BLOCKED
             |         |         |
             v         v         v
       approved-    review-    blocked-
       payments     payments   payments
             |         |         |
             +---------+---------+
                       |
                       v
                  PostgreSQL
                       |
                       v
              Spring Boot Dashboard
                       |
                       v
              Thymeleaf + Chart.js
                       |
                       v
              Fraud Explanation API
                       |
                       v
                Google Gemini AI
                       |
                       v
              AI Explanation Display
```

**Important:** Kafka Streams makes the fraud classification decision using configured rules. Gemini only explains the existing decision; it does not approve, review, or block transactions.

## Technology Stack

| Technology | Purpose |
|---|---|
| Java 21 | Backend development |
| Spring Boot | REST API and application framework |
| Apache Kafka 4.1.1 | Event streaming platform |
| Kafka Streams | Real-time transaction processing |
| PostgreSQL 17 | Transaction data persistence |
| Spring Data JPA | Database integration |
| Docker Compose | Local infrastructure |
| Thymeleaf | Dashboard rendering |
| Bootstrap 5 | Dashboard styling |
| Chart.js | Transaction visualization |
| Spring AI | AI integration |
| Google Gemini API | AI-powered fraud explanations |
| Maven | Build and dependency management |

## Features

### 1. Transaction Processing

- Accept payment transactions through REST APIs.
- Generate transaction timestamps in the backend.
- Publish transaction events to Kafka.
- Use customer ID as the Kafka message key.

### 2. Real-Time Fraud Detection

The system evaluates transactions using the following rules:

| Fraud Rule | Evaluation |
|---|---|
| High transaction amount | Amount above ₹1,00,000 → BLOCKED |
| Location change | Different transaction locations within the window → REVIEW |
| Multiple transactions | More than one transaction within the window → REVIEW |
| Medium transaction amount | Amount above ₹50,000 → REVIEW |
| Normal transaction | No fraud rule triggered → APPROVED |

The system uses Kafka Streams windowed aggregation to evaluate transaction activity within a one-minute time difference, with a 10-second grace period.

### 3. Kafka Topic Routing

Transactions are routed into separate Kafka topics based on their evaluation result.

| Kafka Topic | Purpose |
|---|---|
| transactions-topic | Incoming payment transactions |
| approved-payments | Approved transactions |
| review-payments | Transactions requiring review |
| blocked-payments | Blocked transactions |

### 4. PostgreSQL Integration

Spring Data JPA is used to persist transaction information, including:

- Transaction ID
- Customer ID
- Transaction amount
- Transaction location
- Transaction timestamp
- Fraud evaluation status
- Fraud evaluation reason
- AI-generated explanation

Transaction IDs are unique in the database. Repeated persistence attempts for an existing transaction ID are skipped.

### 5. Monitoring Dashboard

A web dashboard built with Spring Boot, Thymeleaf, Bootstrap, and Chart.js.

Dashboard capabilities:

- Total transaction count
- Approved transaction count
- Transactions under review
- Blocked transaction count
- Recent transaction history
- Fraud distribution doughnut chart
- Transaction search and status filtering
- Transaction details modal
- AI fraud explanation modal

### 6. AI-Powered Fraud Explanation

Google Gemini is integrated using Spring AI to provide explanations for fraud evaluation results.

Capabilities:

- Generate explanations based on transaction details.
- Explain why a transaction was approved, reviewed, or blocked.
- Provide risk assessment context.
- Suggest investigation steps for suspicious transactions.
- Store generated explanations in PostgreSQL.
- Reuse an existing stored explanation instead of generating it again.

**AI decision boundary:** Gemini is used only as an explanation assistant. All transaction classifications are determined by the Kafka Streams fraud detection rules.

## Project Structure

```text
fraud-detection-system/
│
├── src/
│   └── main/
│       ├── java/
│       │   └── com/example/fraud_detection_system/
│       │       │
│       │       ├── controller/
│       │       │   ├── PaymentController.java
│       │       │   ├── DashboardController.java
│       │       │   └── FraudExplanationController.java
│       │       │
│       │       ├── model/
│       │       │   ├── Transaction.java
│       │       │   ├── FraudResult.java
│       │       │   ├── FraudWindowState.java
│       │       │   └── TransactionEntity.java
│       │       │
│       │       ├── producer/
│       │       │   └── TransactionProducer.java
│       │       │
│       │       ├── streams/
│       │       │   └── FraudDetectionStream.java
│       │       │
│       │       ├── repository/
│       │       │   └── TransactionRepository.java
│       │       │
│       │       ├── mapper/
│       │       │   └── TransactionMapper.java
│       │       │
│       │       ├── service/
│       │       │   ├── TransactionPersistenceService.java
│       │       │   ├── DashboardService.java
│       │       │   └── FraudExplanationService.java
│       │       │
│       │       └── FraudDetectionSystemApplication.java
│       │
│       └── resources/
│           ├── templates/
│           │   └── dashboard.html
│           └── application.properties
│
├── docker-compose.yml
├── pom.xml
└── README.md
```

## Prerequisites

Install the following before running the application:

- JDK 21
- Maven 3.9+
- Docker Desktop
- IntelliJ IDEA or another Java IDE
- Postman
- Google Gemini API key

## Running the Application

### Step 1: Clone the repository

```bash
git clone <your-github-repository-url>
cd fraud-detection-system
```

### Step 2: Configure the Gemini API key

The Gemini API key must be configured as an environment variable. It should never be committed to GitHub.

The `application.properties` file uses:

```properties
# Google Gemini API configuration
spring.ai.google.genai.api-key=${GEMINI_API_KEY}
spring.ai.google.genai.chat.options.model=gemini-3.5-flash-lite
spring.ai.google.genai.chat.options.temperature=0.2
```

Configure the environment variable locally in IntelliJ IDEA:

1. Open Run → Edit Configurations.
2. Select your Spring Boot application.
3. Locate Environment variables.
4. Add:

```text
GEMINI_API_KEY=your_actual_gemini_api_key
```

5. Save the configuration.

Never replace the placeholder in the README with your actual API key.

### Step 3: Start Kafka and PostgreSQL

Start the infrastructure using Docker Compose:

```bash
docker compose up -d
```

Verify the running containers:

```bash
docker ps
```

### Step 4: Open Kafka UI

Open:

http://localhost:8081

Use Kafka UI to inspect topics and transaction messages.

### Step 5: Verify PostgreSQL

```bash
docker exec -it fraud-postgres psql -U fraud_user -d fraud_detection_db
```

### Step 6: Build the application

```bash
mvn clean test
```

### Step 7: Run Spring Boot

Run `FraudDetectionSystemApplication.java` from IntelliJ IDEA, or use:

```bash
mvn spring-boot:run
```

Make sure the `GEMINI_API_KEY` environment variable is available to the process.

### Step 8: Open the dashboard

http://localhost:8080/dashboard

## Testing with Postman

Submit payment transactions through the payment REST endpoint defined in `PaymentController`.

Example transaction payload:

```json
{
  "transactionId": "TXN-TEST-101",
  "customerId": "CUST-101",
  "amount": 5000,
  "location": "Bangalore"
}
```

Example scenarios to test:

| Scenario | Amount | Expected Status |
|---|---:|---|
| Normal transaction | ₹5,000 | APPROVED |
| Medium-value transaction | ₹65,000 | REVIEW |
| High-value transaction | ₹1,50,000 | BLOCKED |
| Multiple transactions within one minute | ₹5,000 | REVIEW |
| Location change within the window | ₹5,000 | REVIEW |

For the location-change scenario, submit transactions for the same customer from different locations within the configured window.

### Testing AI Fraud Explanation

The AI explanation endpoint is:

```http
POST /api/transactions/{transactionId}/ai-explanation
```

Example:

```http
POST http://localhost:8080/api/transactions/TXN-TEST-101/ai-explanation
```

The API returns the transaction ID and generated explanation.

The dashboard also provides an **AI Explain** action for individual transactions.

## Kafka UI – Inspecting Topics

You can inspect the individual topics through Kafka UI:

- Incoming transactions: `transactions-topic`
- Approved transactions: `approved-payments`
- Transactions requiring review: `review-payments`
- Blocked transactions: `blocked-payments`

For example, open the `review-payments` topic in Kafka UI to inspect transactions requiring review.

## Security

- Gemini API credentials are configured through environment variables.
- API keys are not stored in source code.
- Local secret configuration files are excluded using `.gitignore`.
- Only synthetic test transaction data should be used with the Gemini API.

Before committing or pushing changes, verify that no API keys or other secrets are staged in Git.

## Future Enhancements

Potential improvements planned for the project:

- Automated unit and integration tests.
- Improved duplicate transaction alerting.
- Kafka retry and dead-letter topic handling.
- Improved database failure recovery.
- More configurable fraud detection rules.
- Additional dashboard charts and filters.
- Historical transaction analysis.
- Live transaction monitoring.
- AI-assisted fraud pattern analysis.

## Learning Objectives

This project demonstrates practical implementation of:

- Event-driven architecture.
- Kafka producers and topics.
- Kafka Streams and windowed aggregation.
- Stream processing and fraud evaluation.
- Spring Boot REST API development.
- Spring Data JPA and PostgreSQL integration.
- Docker-based local infrastructure.
- Dashboard development using Thymeleaf and Chart.js.
- Spring AI integration with Google Gemini.
- Environment-based API credential management.

## Disclaimer

This is a personal learning project developed to demonstrate real-time event processing, fraud detection, dashboard development, and AI integration concepts.

It is not a production banking fraud detection platform.

---

**Project:** Real-Time Payment Fraud Detection System

**Category:** Personal Project / Backend Engineering

**Primary Focus:** Java, Spring Boot, Apache Kafka, Kafka Streams, PostgreSQL, Google Gemini AI