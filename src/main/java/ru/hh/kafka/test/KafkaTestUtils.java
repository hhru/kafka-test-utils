package ru.hh.kafka.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.KafkaContainer;

public class KafkaTestUtils {

  public static KafkaContainer startKafkaContainer() {
    KafkaContainer kafkaContainer = new KafkaContainer().withEmbeddedZookeeper();
    kafkaContainer.start();
    return kafkaContainer;
  }

  public static TestKafka startKafka() {
    return connectToKafka(startKafkaContainer().getBootstrapServers(), Map.of(), Map.of());
  }

  public static TestKafka connectToKafka(String bootstrapServers,
                                         Map<String, Object> consumerConfigsOverwrite,
                                         Map<String, Object> producerConfigsOverwrite,
                                         Duration consumerPoolTimeout) {
    return new TestKafka(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, consumerPoolTimeout);
  }

  public static TestKafka connectToKafka(String bootstrapServers,
                                         Map<String, Object> consumerConfigsOverwrite,
                                         Map<String, Object> producerConfigsOverwrite) {
    return new TestKafka(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite);
  }

  public static TestKafkaWithJsonMessages startKafkaWithJsonMessages(ObjectMapper objectMapper) {
    return connectToKafkaWithJsonMessages(startKafkaContainer().getBootstrapServers(), Map.of(), Map.of(), objectMapper);
  }

  public static TestKafkaWithJsonMessages connectToKafkaWithJsonMessages(String bootstrapServers,
                                                                         Map<String, Object> consumerConfigsOverwrite,
                                                                         Map<String, Object> producerConfigsOverwrite,
                                                                         Duration consumerPoolTimeout,
                                                                         ObjectMapper objectMapper) {
    return new TestKafkaWithJsonMessages(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, consumerPoolTimeout, objectMapper);
  }

  public static TestKafkaWithJsonMessages connectToKafkaWithJsonMessages(String bootstrapServers,
                                                                         Map<String, Object> consumerConfigsOverwrite,
                                                                         Map<String, Object> producerConfigsOverwrite,
                                                                         ObjectMapper objectMapper) {
    return new TestKafkaWithJsonMessages(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, objectMapper);
  }

}
