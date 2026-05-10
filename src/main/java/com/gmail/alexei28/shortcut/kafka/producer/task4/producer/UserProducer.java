package com.gmail.alexei28.shortcut.kafka.producer.task4.producer;

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

  /*
     - не блокируем поток
     - контролируем результат
     - добавить настройки в application.yml (acks, retries)
  */
  public void sendRaw(String value) {
    // Нет идемпотентности на уровне Kafka -> просто каждый раз генерируется новый UUID, и это
    // вообще не решает проблему дубликатов.
    String eventId = UUID.randomUUID().toString();

    kafkaTemplate
        .send(topic, eventId, value)
        .whenComplete(
            (result, ex) -> {
              if (ex != null) {
                logger.error("sendRaw, send failed: {}", ex.getMessage());
              } else {
                logger.info(
                    "sendRaw, successfully sent to topic: {}", result.getRecordMetadata().topic());
              }
            });
  }
}
