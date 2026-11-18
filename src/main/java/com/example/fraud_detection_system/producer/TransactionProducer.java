package com.example.fraud_detection_system.producer;

import com.example.fraud_detection_system.model.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TransactionProducer {

  private static final String TOPIC = "transactions-topic";

  private final KafkaTemplate<String, Transaction> kafkaTemplate;

  public TransactionProducer(KafkaTemplate<String, Transaction> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void publishTransaction(Transaction transaction) {
    kafkaTemplate.send(TOPIC, transaction.customerId(), transaction);
    System.out.println("Transaction published: " + transaction.transactionId());
  }
}
