package ru.hh.kafka.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class TestKafkaWithJsonMessages extends TestKafka {
  private final ObjectMapper objectMapper;

  public TestKafkaWithJsonMessages(String bootstrapServers,
                                   Map<String, Object> consumerConfigsOverwrite,
                                   Map<String, Object> producerConfigsOverwrite,
                                   Duration consumerPoolTimeout,
                                   ObjectMapper objectMapper) {
    super(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, consumerPoolTimeout);
    this.objectMapper = objectMapper;
  }

  public TestKafkaWithJsonMessages(String bootstrapServers,
                                   Map<String, Object> consumerConfigsOverwrite,
                                   Map<String, Object> producerConfigsOverwrite,
                                   ObjectMapper objectMapper) {
    super(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite);
    this.objectMapper = objectMapper;
  }

  public <T> KafkaTopicWatching<T> startJsonTopicWatching(String topic, Class<T> messageClass, Duration getMessagesTimeout) {
    return startTopicWatching(topic, getJsonClassDeserializer(messageClass), getMessagesTimeout);
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
