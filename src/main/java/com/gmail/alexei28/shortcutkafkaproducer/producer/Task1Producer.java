package com.gmail.alexei28.shortcutkafkaproducer.producer;

import com.gmail.alexei28.shortcutkafkaproducer.dto.Task1Dto;
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

  private final KafkaTemplate<String, Task1Dto> kafkaTemplate;

  public Task1Producer(KafkaTemplate<String, Task1Dto> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendTask(Task1Dto task1Dto) {
    sendTask(this.topic, task1Dto);
  }

  public void sendTask(String topic, Task1Dto task1Dto) {
    kafkaTemplate.send(topic, task1Dto);
  }
}
