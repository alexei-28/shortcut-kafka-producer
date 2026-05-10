package com.gmail.alexei28.shortcut.kafka.producer.task2.controller;

import com.gmail.alexei28.shortcut.kafka.producer.task2.dto.CashbackDto;
import com.gmail.alexei28.shortcut.kafka.producer.task2.producer.CashbackProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/cashback")
public class CashbackController {
  private final CashbackProducer cashbackProducer;

  public CashbackController(CashbackProducer cashbackProducer) {
    this.cashbackProducer = cashbackProducer;
  }

  /**
   * Sends a Task2Dto object. E.g. POST http://localhost:8080/api/v1/task2/send
   *
   * <p>Spring Boot auto convert JSON to Task2Dto
   */
  @PostMapping("/send")
  public ResponseEntity<String> sendTask1(@RequestBody CashbackDto cashbackDto) {
    cashbackProducer.sendCashback(cashbackDto);
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
      @PathVariable String topic, @RequestBody CashbackDto cashbackDto) {
    cashbackProducer.sendCashback(topic, cashbackDto);
    return ResponseEntity.ok("Sent to topic: " + topic);
  }
}
