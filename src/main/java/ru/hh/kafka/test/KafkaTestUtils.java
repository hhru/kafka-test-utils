package ru.hh.kafka.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import static java.util.Collections.emptyMap;
import java.util.Map;
import org.testcontainers.containers.KafkaContainer;

public class KafkaTestUtils {

  public static KafkaContainer startKafkaContainer(Map<String, String> additionalConfigs) {
    KafkaContainer kafkaContainer = new KafkaContainer().withEmbeddedZookeeper();
    // Overwrite broker settings using env variables
    // https://docs.confluent.io/current/installation/docker/config-reference.html#confluent-kafka-configuration
    additionalConfigs.forEach((key, value) -> kafkaContainer.withEnv("KAFKA_" + key.replace(".", "_").toUpperCase(), value));
    kafkaContainer.start();
    return kafkaContainer;
  }

  public static TestKafka startKafka() {
    return connectToKafka(startKafkaContainer(emptyMap()).getBootstrapServers(), emptyMap(), emptyMap());
  }

  public static TestKafka startKafka(Map<String, String> additionalConfigs) {
    return connectToKafka(startKafkaContainer(additionalConfigs).getBootstrapServers(), emptyMap(), emptyMap());
  }

  public static TestKafka connectToKafka(String bootstrapServers,
                                         Map<String, Object> consumerConfigsOverwrite,
                                         Map<String, Object> producerConfigsOverwrite,
                                         Duration defaultTopicMonitoringGetMessageTimeout) {
    return new TestKafka(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, defaultTopicMonitoringGetMessageTimeout);
  }

  public static TestKafka connectToKafka(String bootstrapServers,
                                         Map<String, Object> consumerConfigsOverwrite,
                                         Map<String, Object> producerConfigsOverwrite) {
    return new TestKafka(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite);
  }

  public static TestKafkaWithJsonMessages startKafkaWithJsonMessages(ObjectMapper objectMapper) {
    return connectToKafkaWithJsonMessages(startKafkaContainer(emptyMap()).getBootstrapServers(), emptyMap(), emptyMap(), objectMapper);
  }

  public static TestKafkaWithJsonMessages startKafkaWithJsonMessages(ObjectMapper objectMapper, Map<String, String> additionalConfigs) {
    return connectToKafkaWithJsonMessages(startKafkaContainer(additionalConfigs).getBootstrapServers(), emptyMap(), emptyMap(), objectMapper);
  }

  public static TestKafkaWithJsonMessages connectToKafkaWithJsonMessages(String bootstrapServers,
                                                                         Map<String, Object> consumerConfigsOverwrite,
                                                                         Map<String, Object> producerConfigsOverwrite,
                                                                         Duration defaultTopicMonitoringGetMessageTimeout,
                                                                         ObjectMapper objectMapper) {
    return new TestKafkaWithJsonMessages(
        bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, defaultTopicMonitoringGetMessageTimeout, objectMapper
    );
  }

  public static TestKafkaWithJsonMessages connectToKafkaWithJsonMessages(String bootstrapServers,
                                                                         Map<String, Object> consumerConfigsOverwrite,
                                                                         Map<String, Object> producerConfigsOverwrite,
                                                                         ObjectMapper objectMapper) {
    return new TestKafkaWithJsonMessages(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, objectMapper);
  }

}
