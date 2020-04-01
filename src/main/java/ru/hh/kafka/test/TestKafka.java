package ru.hh.kafka.test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestKafka {

  private static Duration DEFAULT_GET_MESSAGES_TIMEOUT = Duration.ofMillis(2000);

  private final String bootstrapServers;
  private final Map<String, Object> consumerConfigsOverwrite;
  private final KafkaProducer<String, byte[]> producer;
  private final Duration defaultMonitoringGetMessageTimeout;

  TestKafka(String bootstrapServers, Map<String, Object> consumerConfigsOverwrite, Map<String, Object> producerConfigsOverwrite,
            Duration defaultTopicMonitoringGetMessageTimeout) {
    this.bootstrapServers = bootstrapServers;
    this.consumerConfigsOverwrite = new HashMap<>(consumerConfigsOverwrite);
    this.producer = new KafkaProducer<>(getProducerConfigs(producerConfigsOverwrite), new StringSerializer(), new ByteArraySerializer());
    this.defaultMonitoringGetMessageTimeout = defaultTopicMonitoringGetMessageTimeout;
  }

  TestKafka(String bootstrapServers, Map<String, Object> consumerConfigsOverwrite, Map<String, Object> producerConfigsOverwrite) {
    this(bootstrapServers, consumerConfigsOverwrite, producerConfigsOverwrite, DEFAULT_GET_MESSAGES_TIMEOUT);
  }

  public void sendMessage(String topic, byte[] message) {
    sendMessage(topic, null, message);
  }

  public void sendMessage(String topic, String key, byte[] message) {
    producer.send(new ProducerRecord<>(topic, key, message));
    producer.flush();
  }

  public <T> KafkaTopicWatching<T> startTopicWatching(String topicName, Deserializer<T> messageDeserializer) {
    return startTopicWatching(topicName, messageDeserializer, defaultMonitoringGetMessageTimeout);
  }

  public <T> KafkaTopicWatching<T> startTopicWatching(String topicName, Deserializer<T> messageDeserializer, Duration getMessagesTimeout) {
    return new KafkaTopicWatching<>(topicName, getConsumerConfigs(), messageDeserializer, getMessagesTimeout);
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
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    return config;
  }

  private Map<String, Object> getDefaultConsumerConfigs() {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID().toString());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return config;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }
}
