package com.example.fraud_detection_system.controller;

import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.producer.TransactionProducer;
import java.time.LocalDateTime;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

  private final TransactionProducer transactionProducer;

  public PaymentController(TransactionProducer transactionProducer) {
    this.transactionProducer = transactionProducer;
  }

  @PostMapping
  public ResponseEntity<String> makePayment(@RequestBody Transaction request) {
    // Generate the transaction timestamp on the backend.
    LocalDateTime currentTime = LocalDateTime.now();

    // Create a transaction with the server-generated timestamp.
    Transaction transaction =
        new Transaction(
            request.transactionId(),
            request.customerId(),
            request.amount(),
            request.location(),
            currentTime);
    transactionProducer.publishTransaction(transaction);
    return ResponseEntity.ok(
        "Payment received and published to kafka:  "
            + transaction.transactionId()
            + " | Transaction Time: "
            + transaction.transactionTime());
  }
}
