package com.gmail.alexei28.shortcut.kafka.producer.task5.producer;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserProducer {
  @Value("${app.kafka.topics.task5}")
  private String topic;

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private static final Logger logger = LoggerFactory.getLogger(UserProducer.class);

  public UserProducer(KafkaTemplate<String, Object> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendRaw(String value) {
    //  Idempotency key
    String eventId = UUID.randomUUID().toString();
    kafkaTemplate.send(topic, eventId, value);
    logger.info(
        "sendRaw, successful sent to Kafka successfully, key = {}, value = {}", eventId, value);
  }
}
