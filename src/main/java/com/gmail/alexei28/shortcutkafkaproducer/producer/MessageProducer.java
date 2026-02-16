package com.gmail.alexei28.shortcutkafkaproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gmail.alexei28.shortcutkafkaproducer.entity.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MessageProducer {
  @Value("${app.kafka.topics.message}")
  private String topic;

  // Используем Object, чтобы иметь возможность отправлять и String (битый JSON)
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;
  private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

  public MessageProducer(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
    this.objectMapper = objectMapper;
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendMessage(Message message) {
    sendMessage(topic, message);
  }

  public void sendMessage(String topic, Message message) {
    try {
      String json = objectMapper.writeValueAsString(message);
      sendValue(topic, json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("sendMessage, Error converting Message to JSON", e);
    }
  }

  /*
     Если в топик придет JSON, который не соответствует структуре UserMessage, Spring выбросит исключение
     (это можно настроить через ErrorHandlingDeserializer).
  */
  public void sendValue(String value) {
    sendValue(topic, value);
  }

  public void sendValue(String topic, String value) {
    logger.info("sendValue, topic: {}, value: {}", topic, value);
    kafkaTemplate.send(topic, value);
  }
}
