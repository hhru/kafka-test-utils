# Инициализация Kafka в unit tests

Позволяет в юнит-тестах поднимать kafka. Умеет писать туда сообщения, мониторить, что находится в топиках.

Примеры использования см. в юнит-тестах TestKafkaWithJsonMessagesTest.java, TestKafkaTest.java


## Пример: добавляем созданную кафку в конфиг приложения, чтобы consumer/producer её увидел
```

@Bean
  public TestKafkaWithJsonMessages testKafka() {
    return KafkaTestUtils.startTestKafkaWithJsonMessages(KafkaSiteObjectMapperFactory.createObjectMapper());
  }

// рассчитываем, что consumer/producer читают настройку "kafka.common.bootstrap.servers" для коннекта к кафке.
@Bean
Properties serviceProperties(TestKafkaWithJsonMessages testKafkaWithJsonMessages) throws IOException {
  PropertiesFactoryBean propertiesFactoryBean = new PropertiesFactoryBean();
  propertiesFactoryBean.setLocation(new ClassPathResource("service-test.properties"));
  propertiesFactoryBean.afterPropertiesSet();
  Properties properties = propertiesFactoryBean.getObject();
  properties.setProperty("kafka.common.bootstrap.servers", testKafkaWithJsonMessages.getBootstrapServers());
  return properties;
}
```
