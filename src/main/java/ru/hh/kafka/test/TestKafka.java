package ru.hh.kafka.test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;

public class TestKafka {

  private final KafkaContainer kafkaContainer;
  private final KafkaProducer<String, byte[]> producer;

  TestKafka(KafkaContainer kafkaContainer) {
    this.kafkaContainer = kafkaContainer;
    this.producer = new KafkaProducer<>(
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
        ),
        new StringSerializer(), new ByteArraySerializer()
    );
  }

  public String getBootstrapServers() {
    return kafkaContainer.getBootstrapServers();
  }

  public <T> KafkaTopicWatching<T> startTopicWatching(String topicName, Deserializer<T> messageDeserializer) {
    return new KafkaTopicWatching<>(topicName, kafkaContainer.getBootstrapServers(), messageDeserializer);
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

}
