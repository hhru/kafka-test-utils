package ru.hh.kafka.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.testcontainers.containers.KafkaContainer;

public class TestKafkaWithJsonMessages extends TestKafka {

  private final ObjectMapper objectMapper;

  TestKafkaWithJsonMessages(KafkaContainer kafkaContainer, ObjectMapper objectMapper) {
    super(kafkaContainer);
    this.objectMapper = objectMapper;
  }

  public <T> KafkaTopicWatching<T> startJsonTopicWatching(String topic, Class<T> messageClass) {
    return startTopicWatching(topic, getJsonClassDeserializer(messageClass));
  }

  public <T> void sendMessage(String topic, T message) {
    sendMessage(topic, null, message);
  }

  public <T> void sendMessage(String topic, String key, T message) {
    try {
      sendMessage(topic, key, objectMapper.writeValueAsBytes(message));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> Deserializer<T> getJsonClassDeserializer(Class<T> messageClass) {
    return (topic, data) -> {
      try {
        return objectMapper.readValue(data, messageClass);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

}
