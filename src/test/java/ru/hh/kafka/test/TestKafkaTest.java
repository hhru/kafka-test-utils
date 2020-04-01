package ru.hh.kafka.test;

import java.time.Duration;
import static java.util.Collections.emptyMap;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestKafkaTest extends TestBase {

  private static final byte[] MESSAGE_CREATED_BEFORE_WATCHING_START = "message created before watching".getBytes();
  private static final byte[] MESSAGE_CREATED_AFTER_WATCHING_START = "message created after watching".getBytes();
  private TestKafka testKafka;
  private String testTopic;

  @BeforeAll
  void setUpTestKafka() {
    testKafka = KafkaTestUtils.connectToKafka(kafkaContainer.getBootstrapServers(), emptyMap(), emptyMap());
  }

  @BeforeEach
  void setUpTestTopic() {
    testTopic = UUID.randomUUID().toString();
  }

  @Test
  void testSingleMessageIsProducedAndReadFromWatcher() {
    KafkaTopicWatching<String> topicWatching = getTopicWatching();

    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    String expectedMessage = new String(MESSAGE_CREATED_BEFORE_WATCHING_START);
    List<String> nextMessages = topicWatching.poolNextMessages();
    assertEquals(1, nextMessages.size());
    assertEquals(expectedMessage, nextMessages.get(0));
  }

  @Test
  void testSeveralMessageAreProducedAndReadFromWatcher() {
    KafkaTopicWatching<String> topicWatching = getTopicWatching();

    testKafka.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);

    List<String> nextMessages = topicWatching.poolNextMessages(4);
    assertEquals(4, nextMessages.size());
  }

  @Test
  void testNoMessagesCreatedBeforeWatchingIsReturned() {
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);

    KafkaTopicWatching<String> topicWatching = getTopicWatching();
    assertEquals(0, topicWatching.poolNextMessages().size());
  }

  @Test
  void testTopicWatcherReturnsEachMessageOnlyOnce() {
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);

    KafkaTopicWatching<String> topicWatching = getTopicWatching();
    byte[] message1 = MESSAGE_CREATED_AFTER_WATCHING_START;
    String expectedMessage1 = new String(message1);
    testKafka.sendMessage(testTopic, message1);
    testKafka.sendMessage(testTopic, message1);
    testKafka.sendMessage(testTopic, message1);
    testKafka.sendMessage(testTopic, message1);

    List<String> nextMessages1 = topicWatching.poolNextMessages(4);
    assertEquals(4, nextMessages1.size());
    assertEquals(expectedMessage1, nextMessages1.get(0));
    assertEquals(expectedMessage1, nextMessages1.get(1));
    assertEquals(expectedMessage1, nextMessages1.get(2));
    assertEquals(expectedMessage1, nextMessages1.get(3));

    byte[] message2 = "message created after watching 2".getBytes();
    String expectedMessage2 = new String(message2);
    testKafka.sendMessage(testTopic, message2);
    testKafka.sendMessage(testTopic, message2);
    testKafka.sendMessage(testTopic, message2);
    testKafka.sendMessage(testTopic, message2);

    List<String> nextMessages2 = topicWatching.poolNextMessages(4);
    assertEquals(4, nextMessages2.size());
    assertEquals(expectedMessage2, nextMessages2.get(0));
    assertEquals(expectedMessage2, nextMessages2.get(1));
    assertEquals(expectedMessage2, nextMessages2.get(2));
    assertEquals(expectedMessage2, nextMessages2.get(3));
  }

  @Test
  void testGetMessagesWithCustomFiltering() {
    KafkaTopicWatching<String> topicWatching = getTopicWatching();
    for (int i = 1; i <= 100; i++) {
      testKafka.sendMessage(testTopic, String.valueOf(i).getBytes());
    }
    List<String> sevenMessagesContainsNumber5 = topicWatching.poolNextMessages(7, m -> m.contains("5"));
    Assertions.assertEquals(7, sevenMessagesContainsNumber5.size());
    Assertions.assertEquals("5", sevenMessagesContainsNumber5.get(0));
    Assertions.assertEquals("15", sevenMessagesContainsNumber5.get(1));
    Assertions.assertEquals("25", sevenMessagesContainsNumber5.get(2));
    Assertions.assertEquals("35", sevenMessagesContainsNumber5.get(3));
    Assertions.assertEquals("45", sevenMessagesContainsNumber5.get(4));
    Assertions.assertEquals("50", sevenMessagesContainsNumber5.get(5));
    Assertions.assertEquals("51", sevenMessagesContainsNumber5.get(6));

    Assertions.assertEquals(51, topicWatching.getAllFoundMessages().size());
    for (int i = 1; i < 51; i++) {
      Assertions.assertEquals(
          Integer.valueOf(topicWatching.getAllFoundMessages().get(i - 1)),
          Integer.parseInt(topicWatching.getAllFoundMessages().get(i)) - 1
      );
    }

    Assertions.assertEquals(20, topicWatching.poolNextMessages(20).size());
    Assertions.assertEquals(51 + 20, topicWatching.getAllFoundMessages().size());

    Assertions.assertEquals(100 - 51 - 20, topicWatching.poolNextMessages(Duration.ofSeconds(1)).size());
    Assertions.assertEquals(100, topicWatching.getAllFoundMessages().size());
  }

  private KafkaTopicWatching<String> getTopicWatching() {
    return testKafka.startTopicWatching(testTopic, new StringDeserializer());
  }

}
