package com.gmail.alexei28.shortcutkafkaproducer.controller;

import com.gmail.alexei28.shortcutkafkaproducer.dto.Task2Dto;
import com.gmail.alexei28.shortcutkafkaproducer.producer.Task2Producer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/task2")
public class Task2Controller {
  private final Task2Producer task2Producer;

  public Task2Controller(Task2Producer task2Producer) {
    this.task2Producer = task2Producer;
  }

  /**
   * Sends a Task2Dto object. E.g. POST http://localhost:8080/api/v1/task2/send
   *
   * <p>Spring Boot auto convert JSON to Task2Dto
   */
  @PostMapping("/send")
  public ResponseEntity<String> sendTask1(@RequestBody Task2Dto task2Dto) {
    task2Producer.sendTask(task2Dto);
    return ResponseEntity.ok("Sent to Kafka successfully");
  }

  /**
   * Sends a Task2Dto object to a specific topic. E.g. POST
   * http://localhost:8080/api/v1/task2/send/at-most-once-topic
   *
   * <p>Spring Boot auto convert JSON to Task2Dto
   */
  @PostMapping("/send/{topic}")
  public ResponseEntity<String> sendToSpecificTopic(
      @PathVariable String topic, @RequestBody Task2Dto task2Dto) {
    task2Producer.sendTask(topic, task2Dto);
    return ResponseEntity.ok("Sent to topic: " + topic);
  }
}
