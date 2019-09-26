package ru.hh.kafka.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestKafkaWithJsonMessagesTest extends TestBase {

  private static final Map<String, String> MESSAGE_CREATED_BEFORE_WATCHING_START = Map.of("test", "message created before watching");
  private static final Map<String, String> MESSAGE_CREATED_AFTER_WATCHING_START = Map.of("test", "message created after watching");
  private TestKafkaWithJsonMessages testKafkaWithJsonMessages;
  private String testTopic;

  @BeforeAll
  void setUpTestKafka() {
    testKafkaWithJsonMessages = KafkaTestUtils.connectToKafkaWithJsonMessages(kafkaContainer.getBootstrapServers(),
        Map.of(), Map.of(), new ObjectMapper());
  }

  @BeforeEach
  void setUpTestTopic() {
    testTopic = UUID.randomUUID().toString();
  }

  @Test
  void testSingleMessageIsProducedAndReadFromWatcher() {
    KafkaTopicWatching<Map> topicWatching = testKafkaWithJsonMessages.startJsonTopicWatching(testTopic, Map.class);

    var expectedMessage = MESSAGE_CREATED_BEFORE_WATCHING_START;
    testKafkaWithJsonMessages.sendMessage(testTopic, expectedMessage);

    List<Map> nextMessages = topicWatching.poolNextMessages();
    assertEquals(1, nextMessages.size());
    assertEquals(expectedMessage, nextMessages.get(0));
  }

  @Test
  void testSeveralMessageAreProducedAndReadFromWatcher() {
    KafkaTopicWatching<Map> topicWatching = testKafkaWithJsonMessages.startJsonTopicWatching(testTopic, Map.class);

    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_AFTER_WATCHING_START);

    List<Map> nextMessages = topicWatching.poolNextMessages(4);
    assertEquals(4, nextMessages.size());
  }

  @Test
  void testNoMessagesCreatedBeforeWatchingIsReturned() {
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);

    KafkaTopicWatching<Map> topicWatching = testKafkaWithJsonMessages.startJsonTopicWatching(testTopic, Map.class);
    assertEquals(0, topicWatching.poolNextMessages().size());
  }

  @Test
  void testTopicWatcherReturnsEachMessageOnlyOnce() {
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);
    testKafkaWithJsonMessages.sendMessage(testTopic, MESSAGE_CREATED_BEFORE_WATCHING_START);

    KafkaTopicWatching<Map> topicWatching = testKafkaWithJsonMessages.startJsonTopicWatching(testTopic, Map.class);
    var message1 = MESSAGE_CREATED_AFTER_WATCHING_START;
    testKafkaWithJsonMessages.sendMessage(testTopic, message1);
    testKafkaWithJsonMessages.sendMessage(testTopic, message1);
    testKafkaWithJsonMessages.sendMessage(testTopic, message1);
    testKafkaWithJsonMessages.sendMessage(testTopic, message1);

    List<Map> nextMessages1 = topicWatching.poolNextMessages(4);
    assertEquals(4, nextMessages1.size());
    assertEquals(message1, nextMessages1.get(0));
    assertEquals(message1, nextMessages1.get(1));
    assertEquals(message1, nextMessages1.get(2));
    assertEquals(message1, nextMessages1.get(3));

    var message2 = Map.of("test", "message created after watching 2");
    testKafkaWithJsonMessages.sendMessage(testTopic, message2);
    testKafkaWithJsonMessages.sendMessage(testTopic, message2);
    testKafkaWithJsonMessages.sendMessage(testTopic, message2);
    testKafkaWithJsonMessages.sendMessage(testTopic, message2);

    List<Map> nextMessages2 = topicWatching.poolNextMessages(4);
    assertEquals(4, nextMessages2.size());
    assertEquals(message2, nextMessages2.get(0));
    assertEquals(message2, nextMessages2.get(1));
    assertEquals(message2, nextMessages2.get(2));
    assertEquals(message2, nextMessages2.get(3));
  }

}
