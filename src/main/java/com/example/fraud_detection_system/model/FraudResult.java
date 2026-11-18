package com.example.fraud_detection_system.model;

public record FraudResult(
    String transactionId, String customerId, double amount, String status, String reason) {}
