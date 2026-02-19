package com.gmail.alexei28.shortcutkafkaproducer.task1.producer;

import com.gmail.alexei28.shortcutkafkaproducer.task1.dto.Task1Dto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/*
   Steps:
   1.Browser (JSON)
   2.Jackson (превращает в Task1Entity)
   3.Controller (получает Task1Entity)
   4.Producer (вызывает MapStruct)
   5.MapStruct (превращает Task1Entity в Task1Dto)
   6.Kafka (отправляет Task1Dto как JSON)
*/
@Service
public class Task1Producer {
  @Value("${app.kafka.topics.task1}")
  private String topic;

  private static final Logger logger = LoggerFactory.getLogger(Task1Producer.class);

  private final KafkaTemplate<String, Task1Dto> kafkaTemplate;

  public Task1Producer(KafkaTemplate<String, Task1Dto> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendTask(Task1Dto task1Dto) {
    sendTask(this.topic, task1Dto);
  }

  /*
     - не блокируем поток
     - контролируем результат
     - добавить настройки в application.yml (acks, retries)
  */
  public void sendTask(String topic, Task1Dto task1Dto) {
    kafkaTemplate
        .send(topic, task1Dto)
        .whenComplete(
            (result, ex) -> {
              if (ex != null) {
                logger.info("sendTask, send failed: {}", ex.getMessage());
              } else {
                logger.info(
                    "sendTask, successfully sent to topic: {}", result.getRecordMetadata().topic());
              }
            });
  }
}
