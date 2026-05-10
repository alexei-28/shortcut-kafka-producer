package com.gmail.alexei28.shortcut.kafka.producer.task1.controller;

import com.gmail.alexei28.shortcut.kafka.producer.task1.dto.Task1Dto;
import com.gmail.alexei28.shortcut.kafka.producer.task1.producer.Task1Producer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/task1")
public class Task1Controller {
  private final Task1Producer task1Producer;

  public Task1Controller(Task1Producer task1Producer) {
    this.task1Producer = task1Producer;
  }

  /*
    Sends a Task1 object. E.g. POST http://localhost:8080/api/v1/task1/send
    Spring Boot auto convert JSON to Task1.
    Example POST request:
    /shortcut-kafka-producer/restclient.rc
  */
  @PostMapping("/send")
  public ResponseEntity<String> sendTask1(@RequestBody Task1Dto task1Dto) {
    task1Producer.sendTask(task1Dto);
    return ResponseEntity.ok("Sent to Kafka successfully");
  }

  /**
   * Sends a Task1 object to a specific topic. E.g. POST
   * http://localhost:8080/api/v1/task1/send/at-most-once-topic
   */
  @PostMapping("/send/{topic}")
  public ResponseEntity<String> sendToSpecificTopic(
      @PathVariable String topic, @RequestBody Task1Dto task1Dto) {
    task1Producer.sendTask(topic, task1Dto);
    return ResponseEntity.ok("Sent to topic: " + topic);
  }

  /**
   * Sends a any string value (e.g., valid/invalid JSON). E.g. POST
   * http://localhost:8080/api/v1/task1/send-value
   */
  /*-
  @PostMapping("/send-value")
  public ResponseEntity<String> sendRawValue(@RequestBody String value) {
    task1Producer.sendTask(value);
    return ResponseEntity.ok("Raw value sent to Kafka successfully");
  }
   */
}
