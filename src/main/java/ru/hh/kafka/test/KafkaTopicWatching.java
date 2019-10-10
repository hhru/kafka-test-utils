package ru.hh.kafka.test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaTopicWatching<T> {

  private static final Duration ASSIGNMENT_ENSURE_POOL_TIMEOUT = Duration.ofMillis(1);
  private static final Duration GET_MESSAGES_POOL_TIMEOUT = Duration.ofMillis(100);
  private static final int ASSIGNMENT_ENSURE_MAX_TRIES = 5000;

  private final KafkaConsumer<String, T> consumer;
  private final Map<TopicPartition, Long> topicPartitionsOffsets;
  private final Duration defaultGetMessagesTimeout;
  private final List<T> allFoundMessages = new ArrayList<>();

  KafkaTopicWatching(String topic, Map<String, Object> consumerConfig, Deserializer<T> valueDeserializer, Duration defaultGetMessagesTimeout) {
    this.defaultGetMessagesTimeout = defaultGetMessagesTimeout;
    this.consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), valueDeserializer);
    this.consumer.subscribe(List.of(topic));
    ensureConsumerAssignment();
    this.topicPartitionsOffsets = consumer.endOffsets(consumer.assignment());
    if (topicPartitionsOffsets.isEmpty()) {
      throw new IllegalStateException("failed to assign consumer to kafka partitions");
    }
  }

  private void ensureConsumerAssignment() {
    for (int i = 0; i < ASSIGNMENT_ENSURE_MAX_TRIES; i++) {
      this.consumer.poll(ASSIGNMENT_ENSURE_POOL_TIMEOUT);
      if (!this.consumer.assignment().isEmpty()) {
        break;
      }
    }
  }

  public List<T> getAllFoundMessages() {
    return List.copyOf(allFoundMessages);
  }

  public List<T> poolNextMessages() {
    return poolNextMessages(null, null, defaultGetMessagesTimeout);
  }

  public List<T> poolNextMessages(int count) {
    return poolNextMessages(count, null, defaultGetMessagesTimeout);
  }

  public List<T> poolNextMessages(Predicate<T> filterPredicate) {
    return poolNextMessages(null, filterPredicate, defaultGetMessagesTimeout);
  }

  public List<T> poolNextMessages(Duration timeout) {
    return poolNextMessages(null, null, timeout);
  }

  public List<T> poolNextMessages(int count, Duration timeout) {
    return poolNextMessages(count, null, timeout);
  }

  public List<T> poolNextMessages(int count, Predicate<T> filterPredicate) {
    return poolNextMessages(count, filterPredicate, defaultGetMessagesTimeout);
  }

  public List<T> poolNextMessages(Predicate<T> filterPredicate, Duration timeout) {
    return poolNextMessages(null, filterPredicate, timeout);
  }

  public Optional<T> poolNextMessage() {
    return poolNextMessage(null, defaultGetMessagesTimeout);
  }

  public Optional<T> poolNextMessage(Predicate<T> filterPredicate) {
    return poolNextMessage(filterPredicate, defaultGetMessagesTimeout);
  }

  public Optional<T> poolNextMessage(Duration timeout) {
    return poolNextMessage(null, timeout);
  }

  public Optional<T> poolNextMessage(Predicate<T> filterPredicate, Duration timeout) {
    List<T> nextMessages = poolNextMessages(1, filterPredicate, timeout);
    if (nextMessages.size() == 0) {
      return Optional.empty();
    }
    return Optional.of(nextMessages.get(0));
  }

  public List<T> poolNextMessages(Integer count, Predicate<T> filterPredicate, Duration timeout) {
    topicPartitionsOffsets.forEach(consumer::seek);
    Instant startTime = Instant.now();
    List<T> currentFoundMessages = new ArrayList<>();
    while (hasRemainingTime(startTime, timeout) && !hasFoundEnoughMessages(currentFoundMessages.size(), count)) {
      ConsumerRecords<String, T> records = consumer.poll(GET_MESSAGES_POOL_TIMEOUT);
      if (records.count() == 0) {
        continue;
      }

      for (ConsumerRecord<String, T> record : records) {
        if (hasFoundEnoughMessages(currentFoundMessages.size(), count)) {
          break;
        }
        topicPartitionsOffsets.computeIfPresent(new TopicPartition(record.topic(), record.partition()), (partition, oldOffset) -> oldOffset + 1);
        T message = record.value();
        allFoundMessages.add(message);
        if (filterPredicate == null || filterPredicate.test(message)) {
          currentFoundMessages.add(message);
        }
      }
      topicPartitionsOffsets.forEach(consumer::seek);
    }
    return currentFoundMessages;
  }

  private boolean hasRemainingTime(Instant startTime, Duration timeout) {
    return Instant.now().isBefore(startTime.plus(timeout));
  }

  private boolean hasFoundEnoughMessages(int foundCount, Integer requiredCount) {
    return requiredCount != null && foundCount >= requiredCount;
  }

  public void close() {
    consumer.close();
  }

}
