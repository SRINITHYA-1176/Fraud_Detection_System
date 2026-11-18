package com.example.fraud_detection_system.model;

public record FraudWindowState(
    // total transactions received from this customer in the window
    long count,
    // Location recorded in the first transaction of the window
    String firstLocation,
    // Becomes true if a transaction arrives from a different location
    boolean locationChanged,
    // Most recently processed transaction
    Transaction latestTransaction) {}
