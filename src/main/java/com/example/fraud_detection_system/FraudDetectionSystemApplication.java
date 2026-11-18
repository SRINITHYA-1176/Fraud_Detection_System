package com.example.fraud_detection_system;

import java.util.TimeZone;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class FraudDetectionSystemApplication {

  public static void main(String[] args) {
    // Force the JVM default timezone to UTC before creating database connections.
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    // Display the timezone used by this Spring Boot JVM.
    System.out.println("Application JVM Timezone: " + java.util.TimeZone.getDefault().getID());

    SpringApplication.run(FraudDetectionSystemApplication.class, args);
  }
}
