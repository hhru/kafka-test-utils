package ru.hh.kafka.test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestKafka {
  private static Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(250);

  private final String bootstrapServers;
  private final Map<String, Object> consumerConfigsOverwrite;
  private final KafkaProducer<String, byte[]> producer;
  private final Duration consumerPoolTimeout;

  TestKafka(String bootstrapServers, Map<String, Object> consumerConfigsOverwrite, Map<String, Object> producerConfigsOverwrite,
            Duration consumerPoolTimeout) {
    this.bootstrapServers = bootstrapServers;
    this.consumerConfigsOverwrite = Map.copyOf(consumerConfigsOverwrite);
    this.producer = new KafkaProducer<>(getProducerConfigs(producerConfigsOverwrite), new StringSerializer(), new ByteArraySerializer());
    this.consumerPoolTimeout = consumerPoolTimeout;
  }

  TestKafka(String bootstrapServers, Map<String, Object> consumerConfigsOverwrite, Map<String, Object> producerConfigsOverwrite) {
    this(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, DEFAULT_POLL_TIMEOUT);
  }

  public void sendMessage(String topic, byte[] message) {
    sendMessage(topic, null, message);
  }

  public void sendMessage(String topic, String key, byte[] message) {
    try {
      producer.send(new ProducerRecord<>(topic, key, message)).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> KafkaTopicWatching<T> startTopicWatching(String topicName, Deserializer<T> messageDeserializer, Duration getMessagesTimeout) {
    return new KafkaTopicWatching<>(topicName, getConsumerConfigs(), messageDeserializer, consumerPoolTimeout, getMessagesTimeout);
  }

  public <T> KafkaTopicWatching<T> startTopicWatching(String topicName, Deserializer<T> messageDeserializer) {
    return startTopicWatching(topicName, messageDeserializer, consumerPoolTimeout.multipliedBy(3).plusMillis(10));
  }

  private Map<String, Object> getProducerConfigs(Map<String, Object> producerConfigsOverwrite) {
    Map<String, Object> producerConfigsFinal = new HashMap<>();
    producerConfigsFinal.putAll(getDefaultProducerConfigs());
    producerConfigsFinal.putAll(producerConfigsOverwrite);
    return producerConfigsFinal;
  }

  private Map<String, Object> getConsumerConfigs() {
    Map<String, Object> consumeConfigsFinal = new HashMap<>();
    consumeConfigsFinal.putAll(getDefaultConsumerConfigs());
    consumeConfigsFinal.putAll(consumerConfigsOverwrite);
    return consumeConfigsFinal;
  }

  private Map<String, Object> getDefaultProducerConfigs() {
    return Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers(),
        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
        ProducerConfig.ACKS_CONFIG, "all"
    );
  }

  private Map<String, Object> getDefaultConsumerConfigs() {
    return Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID().toString(),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"
    );
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }
}
