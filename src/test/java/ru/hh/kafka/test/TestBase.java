package ru.hh.kafka.test;

import static java.util.Collections.emptyMap;
import org.testcontainers.containers.KafkaContainer;

public class TestBase {

  protected static KafkaContainer kafkaContainer = KafkaTestUtils.startKafkaContainer(emptyMap());

}
