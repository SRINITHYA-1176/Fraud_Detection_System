package com.example.fraud_detection_system.controller;

import com.example.fraud_detection_system.service.FraudExplanationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/transactions")
public class FraudExplanationController {
  private final FraudExplanationService fraudExplanationService;

  // Constructor injection for the AI explanation service.
  public FraudExplanationController(FraudExplanationService fraudExplanationService) {
    this.fraudExplanationService = fraudExplanationService;
  }

  // NEW: Generate an AI explanation for a transaction.
  @PostMapping("/{transactionId}/ai-explanation")
  public ResponseEntity<FraudExplanationResponse> generateExplanation(
      @PathVariable String transactionId) {

    String explanation = fraudExplanationService.generateExplanation(transactionId);

    return ResponseEntity.ok(new FraudExplanationResponse(transactionId, explanation));
  }

  // NEW: Response structure returned to the client.
  public record FraudExplanationResponse(String transactionId, String explanation) {}
}
