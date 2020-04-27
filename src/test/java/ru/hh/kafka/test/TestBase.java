package ru.hh.kafka.test;

import java.util.Collections;
import org.testcontainers.containers.KafkaContainer;

public class TestBase {

  protected static KafkaContainer kafkaContainer = KafkaTestUtils.startKafkaContainer(Collections.emptyMap());

}
