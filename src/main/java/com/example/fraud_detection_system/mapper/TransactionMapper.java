package com.example.fraud_detection_system.mapper;

import com.example.fraud_detection_system.model.FraudResult;
import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.model.TransactionEntity;
import org.springframework.stereotype.Component;

@Component
public class TransactionMapper {
  // Convert Kafka transaction and fraud decision into a database entity
  public TransactionEntity toEntity(Transaction transaction, FraudResult fraudResult) {

    TransactionEntity entity = new TransactionEntity();

    // Map transaction details
    entity.setTransactionId(transaction.transactionId());
    entity.setCustomerId(transaction.customerId());
    entity.setAmount(transaction.amount());
    entity.setLocation(transaction.location());
    entity.setTransactionTime(transaction.transactionTime());

    // Map fraud detection decision
    entity.setStatus(fraudResult.status());
    entity.setReason(fraudResult.reason());

    return entity;
  }
}
