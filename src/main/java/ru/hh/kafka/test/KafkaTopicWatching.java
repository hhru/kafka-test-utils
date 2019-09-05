package ru.hh.kafka.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaTopicWatching<T> {

  private static final int DEFAULT_EXPECTED_COUNT = 1;
  private static Duration POLL_TIMEOUT = Duration.ofMillis(250);
  private static Duration COMMIT_TIMEOUT = Duration.ofMillis(250);
  private static Duration DEFAULT_GET_MESSAGES_TIMEOUT = Duration.ofMillis(750);

  private final KafkaConsumer<String, T> consumer;

  protected KafkaTopicWatching(String topic, String bootstrapServers, Deserializer<T> valueDeserializer) {
    consumer = new KafkaConsumer<>(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ),
        new StringDeserializer(),
        valueDeserializer
    );
    consumer.subscribe(List.of(topic));
    consumer.seekToEnd(Set.of());
    consumer.commitSync(COMMIT_TIMEOUT);
    consumer.poll(POLL_TIMEOUT);
  }

  public List<T> getNextMessages() {
    return getNextMessages(DEFAULT_GET_MESSAGES_TIMEOUT, DEFAULT_EXPECTED_COUNT);
  }

  public List<T> getNextMessages(int expectedCount) {
    return getNextMessages(DEFAULT_GET_MESSAGES_TIMEOUT, expectedCount);
  }

  public List<T> getNextMessages(Duration timeout) {
    return getNextMessages(timeout, DEFAULT_EXPECTED_COUNT);
  }

  public List<T> getNextMessages(Duration timeout, int expectedCount) {
    long startTime = System.currentTimeMillis();
    List<T> foundMessages = new ArrayList<>();
    while (System.currentTimeMillis() - startTime < timeout.toMillis() && foundMessages.size() < expectedCount) {
      ConsumerRecords<String, T> records = consumer.poll(POLL_TIMEOUT);

      if (records.count() == 0) {
        continue;
      }

      records.forEach(record -> foundMessages.add(record.value()));
      consumer.commitSync(COMMIT_TIMEOUT);
    }
    return foundMessages;
  }

}
