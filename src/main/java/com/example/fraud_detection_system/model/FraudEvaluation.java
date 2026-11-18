package com.example.fraud_detection_system.model;

// NEW: Carries the original transaction and its fraud decision together.
// This allows us to persist both transaction details and evaluation results.
public record FraudEvaluation(Transaction transaction, FraudResult fraudResult) {}

/*Previously, our stream produced only FraudResult. But PostgreSQL needs both:
        - Original transaction details: customer ID, amount, location, transaction time.
- Fraud decision: APPROVED, REVIEW, or BLOCKED, including the reason.*/
