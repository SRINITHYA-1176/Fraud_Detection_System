package com.example.fraud_detection_system.service;

import com.example.fraud_detection_system.model.TransactionEntity;
import com.example.fraud_detection_system.repository.TransactionRepository;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class DashboardService {
  private final TransactionRepository transactionRepository;

  // Constructor injection
  public DashboardService(TransactionRepository transactionRepository) {
    this.transactionRepository = transactionRepository;
  }

  // Fetch transaction statistics from PostgreSQL
  public Map<String, Long> getDashboardStatistics() {

    return Map.of(
        "totalTransactions", transactionRepository.count(),
        "approvedTransactions", transactionRepository.countByStatus("APPROVED"),
        "reviewTransactions", transactionRepository.countByStatus("REVIEW"),
        "blockedTransactions", transactionRepository.countByStatus("BLOCKED"));
  }

  // NEW: Fetch the latest 10 transactions for the dashboard.
  public List<TransactionEntity> getRecentTransactions() {
    return transactionRepository.findTop10ByOrderByTransactionTimeDesc();
  }
}
