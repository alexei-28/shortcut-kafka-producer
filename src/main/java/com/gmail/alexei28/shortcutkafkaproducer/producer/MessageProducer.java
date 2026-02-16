package com.gmail.alexei28.shortcutkafkaproducer.producer;

import com.gmail.alexei28.shortcutkafkaproducer.dto.Message;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class MessageProducer {
  @Value("${app.kafka.topics.message}")
  private String topic;

  // Используем Object, чтобы иметь возможность отправлять и String (битый JSON)
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

  public MessageProducer(KafkaTemplate<String, Object> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /*
     Если в топик придет JSON, который не соответствует структуре UserMessage, Spring выбросит исключение
     (это можно настроить через ErrorHandlingDeserializer).
  */
  // Метод специально для тестов или отправки сырых данных
  public void sendRaw(String kafkaTopic, String rawData) {
    logger.info("sendRaw, kafkaTopic: {}, rawData: {}", kafkaTopic, rawData);
    kafkaTemplate.send(kafkaTopic, rawData);
  }

  public void sendMessage(Message message) {
    sendMessage(topic, message);
  }

  public void sendMessage(String kafkaTopic, Message message) {
    CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(kafkaTopic, message);
    future.whenComplete(
        (sendResult, ex) -> {
          if (ex == null) {
            logger.info("sendMessageLog, sendResult: {}", sendResult);
          } else {
            logger.error("sendMessageLog, Error", ex);
          }
        });
  }
}
