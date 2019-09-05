package ru.hh.kafka.test;

import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
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
    testKafka = KafkaTestUtils.startTestKafka(kafkaContainer);
  }

  @BeforeEach
  void setUpTestTopic() {
    testTopic = UUID.randomUUID().toString();
  }

  @Test
  void testSingleMessageIsProducedAndReadFromWatcher() {
    KafkaTopicWatching<String> topicWatching = getTopicWatching();

    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    var expectedMessage = new String(MESSAGE_CREATED_BEFORE_WATCHING_START);
    List<String> nextMessages = topicWatching.getNextMessages();
    Assertions.assertEquals(1, nextMessages.size());
    Assertions.assertEquals(expectedMessage, nextMessages.get(0));
  }

  @Test
  void testSeveralMessageAreProducedAndReadFromWatcher() {
    KafkaTopicWatching<String> topicWatching = getTopicWatching();

    testKafka.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);

    List<String> nextMessages = topicWatching.getNextMessages(4);
    Assertions.assertEquals(4, nextMessages.size());
  }

  @Test
  void testNoMessagesCreatedBeforeWatchingIsReturned() {
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);

    KafkaTopicWatching<String> topicWatching = getTopicWatching();
    Assertions.assertEquals(0, topicWatching.getNextMessages().size());
  }

  @Test
  void testTopicWatcherReturnsEachMessageOnlyOnce() {
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafka.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);

    KafkaTopicWatching<String> topicWatching = getTopicWatching();
    var message1 = MESSAGE_CREATED_AFTER_WATCHING_START;
    var expectedMessage1 = new String(message1);
    testKafka.sendMessage(testTopic, message1);
    testKafka.sendMessage(testTopic, message1);
    testKafka.sendMessage(testTopic, message1);
    testKafka.sendMessage(testTopic, message1);

    List<String> nextMessages1 = topicWatching.getNextMessages(4);
    Assertions.assertEquals(4, nextMessages1.size());
    Assertions.assertEquals(expectedMessage1, nextMessages1.get(0));
    Assertions.assertEquals(expectedMessage1, nextMessages1.get(1));
    Assertions.assertEquals(expectedMessage1, nextMessages1.get(2));
    Assertions.assertEquals(expectedMessage1, nextMessages1.get(3));

    var message2 = "message created after watching 2".getBytes();
    var expectedMessage2 = new String(message2);
    testKafka.sendMessage(testTopic, message2);
    testKafka.sendMessage(testTopic, message2);
    testKafka.sendMessage(testTopic, message2);
    testKafka.sendMessage(testTopic, message2);

    List<String> nextMessages2 = topicWatching.getNextMessages(4);
    Assertions.assertEquals(4, nextMessages2.size());
    Assertions.assertEquals(expectedMessage2, nextMessages2.get(0));
    Assertions.assertEquals(expectedMessage2, nextMessages2.get(1));
    Assertions.assertEquals(expectedMessage2, nextMessages2.get(2));
    Assertions.assertEquals(expectedMessage2, nextMessages2.get(3));
  }

  private KafkaTopicWatching<String> getTopicWatching() {
    return testKafka.startTopicWatching(testTopic, new StringDeserializer());
  }

}
