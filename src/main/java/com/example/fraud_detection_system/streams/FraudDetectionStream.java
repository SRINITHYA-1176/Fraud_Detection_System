package com.example.fraud_detection_system.streams;

import com.example.fraud_detection_system.model.FraudEvaluation;
import com.example.fraud_detection_system.model.FraudResult;
import com.example.fraud_detection_system.model.FraudWindowState;
import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.service.TransactionPersistenceService;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class FraudDetectionStream {

  // Kafka topic from which incoming payment transactions are consumed.
  private static final String INPUT_TOPIC = "transactions-topic";

  // NEW: Service that saves transaction details and fraud decisions in PostgreSQL.
  private final TransactionPersistenceService persistenceService;

  // NEW: Constructor injection allows Spring to provide the persistence service.
  public FraudDetectionStream(TransactionPersistenceService persistenceService) {
    this.persistenceService = persistenceService;
  }

  @Bean
  public KStream<String, Transaction> fraudDetectionPipeline(StreamsBuilder builder) {

    // ---------------------------------------------------------
    // STEP 1: CONSUME TRANSACTIONS FROM KAFKA
    // ---------------------------------------------------------

    // Read incoming transaction messages from transactions-topic.
    // StringSerde deserializes the Kafka message key (customerId).
    // JsonSerde converts the JSON message value into a Transaction object.
    KStream<String, Transaction> transactionKStream =
        builder.stream(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), new JsonSerde<>(Transaction.class).ignoreTypeHeaders()));

    // ---------------------------------------------------------
    // STEP 2: GROUP TRANSACTIONS BY CUSTOMER
    // ---------------------------------------------------------

    // The producer sends customerId as the Kafka message key.
    // groupByKey() groups transactions using that existing key.
    //
    // Example:
    // Customer C101 -> Transaction 1, Transaction 2, Transaction 3
    //
    // This allows us to detect repeated transactions by the same customer.
    var groupedTransactions =
        transactionKStream.groupByKey(
            Grouped.with(Serdes.String(), new JsonSerde<>(Transaction.class).ignoreTypeHeaders()));

    // ---------------------------------------------------------
    // STEP 3: DEFINE A ONE-MINUTE TUMBLING WINDOW
    // ---------------------------------------------------------

    // A tumbling window divides transaction time into fixed,
    // non-overlapping one-minute intervals.
    //
    // Example:
    // 10:00:00 - 10:00:59 -> First window
    // 10:01:00 - 10:01:59 -> Second window
    //
    // CHANGED: Use a sliding window to detect transactions
    // occurring within a rolling one-minute time difference.
    SlidingWindows slidingWindow =
        SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10));

    // ---------------------------------------------------------
    // STEP 4: AGGREGATE TRANSACTIONS WITHIN EACH WINDOW
    // ---------------------------------------------------------

    // Maintain a separate FraudWindowState for each customer
    // in each one-minute window.
    //
    // Initial state:
    // count = 0
    // firstLocation = null
    // locationChanged = false
    // latestTransaction = null
    var transactionWindowState =
        groupedTransactions
            .windowedBy(slidingWindow)
            .aggregate(

                // Create the initial state for a new customer window.
                () -> new FraudWindowState(0, null, false, null),

                // This function runs whenever a new transaction
                // arrives for the same customer and window.
                (customerId, transaction, currentState) -> {

                  // Capture the first transaction's location.
                  // For later transactions, retain the original location.
                  String firstLocation =
                      currentState.count() == 0
                          ? transaction.location()
                          : currentState.firstLocation();

                  // Check whether the new transaction comes
                  // from a different location than the first one.
                  //
                  // Once locationChanged becomes true,
                  // it remains true for the rest of this window.
                  boolean locationChanged =
                      currentState.locationChanged()
                          || (firstLocation != null
                              && transaction.location() != null
                              && !firstLocation.equalsIgnoreCase(transaction.location()));

                  // Create the updated state:
                  // 1. Increase transaction count.
                  // 2. Preserve the first location.
                  // 3. Preserve the location-change flag.
                  // 4. Store the latest transaction.
                  return new FraudWindowState(
                      currentState.count() + 1, firstLocation, locationChanged, transaction);
                },

                // Configure how Kafka Streams stores the aggregated state.
                // StringSerde handles the customer key.
                // JsonSerde handles the FraudWindowState value.
                Materialized.with(
                    Serdes.String(), new JsonSerde<>(FraudWindowState.class).noTypeInfo()));

    // ---------------------------------------------------------
    // STEP 5: CONVERT AGGREGATED STATE INTO A STREAM
    // ---------------------------------------------------------

    // aggregate() produces a KTable containing the latest state.
    // toStream() converts its updates into stream records.
    //
    // Each update contains:
    // - Windowed customer key
    // - Updated FraudWindowState
    KStream<Windowed<String>, FraudWindowState> windowedStream = transactionWindowState.toStream();

    // ---------------------------------------------------------
    // STEP 6: APPLY FRAUD DETECTION RULES
    // ---------------------------------------------------------

    // CHANGED: Previously, this step returned only FraudResult.
    // Now it returns FraudEvaluation, which contains BOTH:
    // - Original Transaction
    // - FraudResult
    //
    // We need the original transaction later when saving to PostgreSQL.
    KStream<Windowed<String>, FraudEvaluation> evaluationStream =
        windowedStream.mapValues(
            state -> {

              // Get the latest transaction from the aggregated state.
              Transaction transaction = state.latestTransaction();

              System.out.println(
                  "Evaluating transaction: "
                      + transaction.transactionId()
                      + " | Customer: "
                      + transaction.customerId()
                      + " | Count: "
                      + state.count()
                      + " | Amount: "
                      + transaction.amount());

              FraudResult fraudResult;

              // RULE 1: Block transactions above 1 lakh.
              if (transaction.amount() > 100000) {

                fraudResult =
                    new FraudResult(
                        transaction.transactionId(),
                        transaction.customerId(),
                        transaction.amount(),
                        "BLOCKED",
                        "Transaction amount exceeds 1 Lakh");

                // RULE 2: Send transactions from different locations for review.
              } else if (state.locationChanged()) {

                fraudResult =
                    new FraudResult(
                        transaction.transactionId(),
                        transaction.customerId(),
                        transaction.amount(),
                        "REVIEW",
                        "Customer made transactions from different locations");

                // RULE 3: Send repeated transactions within one minute for review.
              } else if (state.count() > 1) {

                fraudResult =
                    new FraudResult(
                        transaction.transactionId(),
                        transaction.customerId(),
                        transaction.amount(),
                        "REVIEW",
                        "Customer made multiple transactions within 1 minute");

                // RULE 4: Send transactions above 50,000 for review.
              } else if (transaction.amount() > 50000) {

                fraudResult =
                    new FraudResult(
                        transaction.transactionId(),
                        transaction.customerId(),
                        transaction.amount(),
                        "REVIEW",
                        "Transaction amount exceeds 50,000");

                // RULE 5: Approve transactions that pass all checks.
              } else {

                fraudResult =
                    new FraudResult(
                        transaction.transactionId(),
                        transaction.customerId(),
                        transaction.amount(),
                        "APPROVED",
                        "Transaction passed all fraud checks");
              }

              // NEW: Package the transaction and its decision together.
              // This allows the next step to save the complete record in PostgreSQL.
              return new FraudEvaluation(transaction, fraudResult);
            });

    // ---------------------------------------------------------
    // STEP 7: RESTORE CUSTOMER ID AS THE KAFKA MESSAGE KEY
    // ---------------------------------------------------------

    // After windowing, the key is Windowed<String>.
    // selectKey() extracts the original customerId from that windowed key.
    //
    // Result:
    // Key   -> customerId
    // Value -> FraudEvaluation
    KStream<String, FraudEvaluation> resultEvaluationStream =
        evaluationStream.selectKey((windowedKey, evaluation) -> windowedKey.key());

    // ---------------------------------------------------------
    // STEP 8: SAVE TRANSACTION AND FRAUD RESULT TO POSTGRESQL
    // ---------------------------------------------------------

    // NEW: For every evaluation, call the persistence service.
    //
    // The service:
    // 1. Maps Transaction + FraudResult into TransactionEntity.
    // 2. Saves the entity using TransactionRepository.
    // 3. Stores the record in the PostgreSQL transactions table.
    //
    // Important: This database write is a side effect.
    // It is not part of an atomic Kafka + PostgreSQL transaction.
    resultEvaluationStream.foreach(
        (customerId, evaluation) ->
            persistenceService.saveTransaction(evaluation.transaction(), evaluation.fraudResult()));

    // ---------------------------------------------------------
    // STEP 9: EXTRACT FRAUD RESULT FOR KAFKA OUTPUT TOPICS
    // ---------------------------------------------------------

    // NEW: PostgreSQL persistence needs the full FraudEvaluation.
    // Kafka output topics only need the FraudResult.
    //
    // mapValues() keeps the customerId key unchanged
    // and extracts only the FraudResult from FraudEvaluation.
    KStream<String, FraudResult> fraudResultStream =
        resultEvaluationStream.mapValues(FraudEvaluation::fraudResult);

    // ---------------------------------------------------------
    // STEP 10: ROUTE RESULTS TO DIFFERENT KAFKA TOPICS
    // ---------------------------------------------------------

    // Send approved payments to approved-payments topic.
    fraudResultStream
        .filter((customerId, result) -> result.status().equals("APPROVED"))
        .to(
            "approved-payments",
            Produced.with(Serdes.String(), new JsonSerde<>(FraudResult.class).noTypeInfo()));

    // Send suspicious payments to review-payments topic.
    fraudResultStream
        .filter((customerId, result) -> result.status().equals("REVIEW"))
        .to(
            "review-payments",
            Produced.with(Serdes.String(), new JsonSerde<>(FraudResult.class).noTypeInfo()));

    // Send high-risk payments to blocked-payments topic.
    fraudResultStream
        .filter((customerId, result) -> result.status().equals("BLOCKED"))
        .to(
            "blocked-payments",
            Produced.with(Serdes.String(), new JsonSerde<>(FraudResult.class).noTypeInfo()));

    // Return the original stream.
    return transactionKStream;
  }
}
