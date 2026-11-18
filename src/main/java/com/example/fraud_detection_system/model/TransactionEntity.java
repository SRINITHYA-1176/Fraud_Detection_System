package com.example.fraud_detection_system.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "transactions")
@Getter
@Setter
@NoArgsConstructor
public class TransactionEntity {

  // Primary key generated automatically by PostgreSQL
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  // Unique business transaction ID
  @Column(nullable = false, unique = true)
  private String transactionId;

  // Customer who initiated the transaction
  @Column(nullable = false)
  private String customerId;

  // Transaction amount
  @Column(nullable = false)
  private double amount;

  // Transaction location
  private String location;

  // Time when the transaction was received
  private LocalDateTime transactionTime;

  // Fraud detection decision
  private String status;

  // Explanation for the decision
  private String reason;

  //  Stores the AI-generated fraud explanation.
  @Column(length = 2000)
  private String aiExplanation;
}
