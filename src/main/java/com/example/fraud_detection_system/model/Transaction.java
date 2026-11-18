package com.example.fraud_detection_system.model;

import java.time.LocalDateTime;

public record Transaction(
    String transactionId,
    String customerId,
    double amount,
    String location,
    LocalDateTime transactionTime) {}
