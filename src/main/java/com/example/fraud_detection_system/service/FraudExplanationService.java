package com.example.fraud_detection_system.service;

import com.example.fraud_detection_system.model.TransactionEntity;
import com.example.fraud_detection_system.repository.TransactionRepository;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class FraudExplanationService {
  private final ChatClient chatClient;
  private final TransactionRepository transactionRepository;

  // Constructor injection for Gemini ChatClient and PostgreSQL repository.
  public FraudExplanationService(
      ChatClient.Builder chatClientBuilder, TransactionRepository transactionRepository) {

    this.chatClient = chatClientBuilder.build();
    this.transactionRepository = transactionRepository;
  }

  // Generate an AI explanation for an existing transaction.
  @Transactional
  public String generateExplanation(String transactionId) {

    // Step 1: Fetch transaction details from PostgreSQL.
    TransactionEntity transaction =
        transactionRepository
            .findByTransactionId(transactionId)
            .orElseThrow(
                () -> new IllegalArgumentException("Transaction not found: " + transactionId));

    // Step 2: Return the existing explanation if already generated.
    if (transaction.getAiExplanation() != null && !transaction.getAiExplanation().isBlank()) {

      return transaction.getAiExplanation();
    }

    // Step 3: Prepare transaction details for Gemini.
    String prompt =
        """
                Explain the following payment transaction in simple,
                professional language for a fraud investigation dashboard.

                Transaction ID: %s
                Customer ID: %s
                Amount: %.2f
                Location: %s
                Transaction Time: %s
                Fraud Status: %s
                Fraud Detection Reason: %s

                Instructions:
                1. Explain why the existing fraud detection rule classified
                   this transaction with the given status and reason.
                2. Explain the risk in simple language.
                3. Suggest a reasonable investigation or verification step.
                4. Do not change or override the existing fraud status.
                5. Do not invent transaction details or claim facts that
                   are not provided.
                6. Keep the explanation concise.
                """
            .formatted(
                transaction.getTransactionId(),
                transaction.getCustomerId(),
                transaction.getAmount(),
                transaction.getLocation(),
                transaction.getTransactionTime(),
                transaction.getStatus(),
                transaction.getReason());

    // Step 4: Send the prompt to Google Gemini.
    String explanation =
        chatClient
            .prompt()
            .system("You are an assistant helping payment fraud investigators.")
            .user(prompt)
            .call()
            .content();

    // Step 5: Validate the AI response.
    if (explanation == null || explanation.isBlank()) {
      throw new IllegalStateException("Gemini returned an empty fraud explanation.");
    }

    // Step 6: Save the explanation in PostgreSQL.
    transaction.setAiExplanation(explanation.trim());
    transactionRepository.save(transaction);

    return explanation.trim();
  }
}
