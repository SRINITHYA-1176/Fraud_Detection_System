package com.example.fraud_detection_system.service;

import com.example.fraud_detection_system.mapper.TransactionMapper;
import com.example.fraud_detection_system.model.FraudResult;
import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.model.TransactionEntity;
import com.example.fraud_detection_system.repository.TransactionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionPersistenceService {
  private final TransactionRepository transactionRepository;
  private final TransactionMapper transactionMapper;

  // Constructor injection for repository and mapper
  public TransactionPersistenceService(
      TransactionRepository transactionRepository, TransactionMapper transactionMapper) {
    this.transactionRepository = transactionRepository;
    this.transactionMapper = transactionMapper;
  }

  // Convert and save the evaluated transaction into PostgreSQL
  @Transactional
  public void saveTransaction(Transaction transaction, FraudResult fraudResult) {

    // NEW: Avoid inserting the same transaction more than once.
    if (transactionRepository.existsByTransactionId(transaction.transactionId())) {
      System.out.println("Duplicate transaction skipped: " + transaction.transactionId());
      return;
    }

    // Convert Kafka transaction + fraud decision into a JPA entity.
    TransactionEntity entity = transactionMapper.toEntity(transaction, fraudResult);

    // Persist the entity in PostgreSQL.
    transactionRepository.save(entity);

    System.out.println(
        "Transaction saved to PostgreSQL: "
            + entity.getTransactionId()
            + " | Status: "
            + entity.getStatus());
  }
}
