package com.example.fraud_detection_system.repository;

import com.example.fraud_detection_system.model.TransactionEntity;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

// Spring Data JPA automatically implements database operations
public interface TransactionRepository extends JpaRepository<TransactionEntity, Long> {

  // count transactions by status
  long countByStatus(String status);

  // Count all transactions
  long count();

  // NEW: Check whether a transaction has already been saved.
  boolean existsByTransactionId(String transactionId);

  // NEW: Fetch the latest 10 transactions based on transaction time.
  List<TransactionEntity> findTop10ByOrderByTransactionTimeDesc();

  // NEW: Find a transaction using its transaction ID.
  Optional<TransactionEntity> findByTransactionId(String transactionId);
}
