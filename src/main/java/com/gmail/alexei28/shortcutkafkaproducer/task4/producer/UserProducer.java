package com.gmail.alexei28.shortcutkafkaproducer.task4.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserProducer {
  @Value("${app.kafka.topics.task4}")
  private String topic;

  private final ObjectMapper objectMapper;
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private static final Logger logger = LoggerFactory.getLogger(UserProducer.class);

  public UserProducer(ObjectMapper objectMapper, KafkaTemplate<String, Object> kafkaTemplate) {
    this.objectMapper = objectMapper;
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
