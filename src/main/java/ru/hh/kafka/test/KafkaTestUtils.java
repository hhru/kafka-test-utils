package ru.hh.kafka.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.containers.KafkaContainer;

public class KafkaTestUtils {

  public static KafkaContainer startKafkaContainer() {
    KafkaContainer kafkaContainer = new KafkaContainer().withEmbeddedZookeeper();
    kafkaContainer.start();
    return kafkaContainer;
  }

  public static TestKafka startTestKafka(KafkaContainer kafkaContainer) {
    return new TestKafka(kafkaContainer);
  }

  public static TestKafka startTestKafka() {
    return startTestKafka(startKafkaContainer());
  }

  public static TestKafkaWithJsonMessages startTestKafkaWithJsonMessages(KafkaContainer kafkaContainer, ObjectMapper objectMapper) {
    return new TestKafkaWithJsonMessages(kafkaContainer, objectMapper);
  }

  public static TestKafkaWithJsonMessages startTestKafkaWithJsonMessages(ObjectMapper objectMapper) {
    return startTestKafkaWithJsonMessages(startKafkaContainer(), objectMapper);
  }

}
