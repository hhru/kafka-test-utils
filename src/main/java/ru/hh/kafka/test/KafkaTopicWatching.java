package ru.hh.kafka.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaTopicWatching<T> {
  private static final int DEFAULT_EXPECTED_COUNT = 1;

  private final KafkaConsumer<String, T> consumer;
  private final Map<TopicPartition, Long> topicPartitionsOffsets;
  private final Duration poolTimeout;
  private final Duration getMessagesTimeout;

  KafkaTopicWatching(String topic, Map<String, Object> consumerConfig, Deserializer<T> valueDeserializer,
                     Duration poolTimeout, Duration getMessagesTimeout) {
    this.poolTimeout = poolTimeout;
    this.getMessagesTimeout = getMessagesTimeout;

    this.consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), valueDeserializer);
    this.consumer.subscribe(List.of(topic));
    this.consumer.poll(poolTimeout);
    this.topicPartitionsOffsets = consumer.endOffsets(consumer.assignment());

    this.consumer.commitSync();
  }

  public List<T> getNextMessages() {
    return getNextMessages(DEFAULT_EXPECTED_COUNT);
  }

  public List<T> getNextMessages(int expectedCount) {
    topicPartitionsOffsets.forEach(consumer::seek);
    long startTime = System.currentTimeMillis();
    List<T> foundMessages = new ArrayList<>();
    while (System.currentTimeMillis() - startTime < getMessagesTimeout.toMillis() && foundMessages.size() < expectedCount) {
      ConsumerRecords<String, T> records = consumer.poll(poolTimeout);
      if (records.count() == 0) {
        continue;
      }

      records.forEach(record -> {
        foundMessages.add(record.value());
        topicPartitionsOffsets.computeIfPresent(new TopicPartition(record.topic(), record.partition()), (partition, oldOffset) -> oldOffset + 1);
      });
      topicPartitionsOffsets.forEach(consumer::seek);
    }
    return foundMessages;
  }

}
