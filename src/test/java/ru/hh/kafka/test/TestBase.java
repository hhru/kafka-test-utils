package ru.hh.kafka.test;

import org.testcontainers.containers.KafkaContainer;

public class TestBase {

  protected static KafkaContainer kafkaContainer = KafkaTestUtils.startKafkaContainer();

}
