package com.gmail.alexei28.shortcutkafkaproducer.producer;

import com.gmail.alexei28.shortcutkafkaproducer.dto.Task2Dto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/*
   Steps:
   1.Browser (JSON)
   2.Jackson (превращает в Task2)
   3.Controller (получает Task2)
   4.Producer (вызывает MapStruct)
   5.MapStruct (превращает Task2 в Task2Dto)
   6.Kafka (отправляет Task2Dto как JSON)
*/
@Service
public class Task2Producer {
  @Value("${app.kafka.topics.task2}")
  private String topic;

  private final KafkaTemplate<String, Task2Dto> kafkaTemplate;

  public Task2Producer(KafkaTemplate<String, Task2Dto> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendTask(Task2Dto task2Dto) {
    sendTask(this.topic, task2Dto);
  }

  public void sendTask(String topic, Task2Dto task2Dto) {
    kafkaTemplate.send(topic, task2Dto);
  }
}
