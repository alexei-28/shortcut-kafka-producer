package com.gmail.alexei28.shortcutkafkaproducer.task3.controller;

import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.TransferDto;
import com.gmail.alexei28.shortcutkafkaproducer.task3.producer.TransferProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/transfer")
public class TransferController {
  private final TransferProducer transferProducer;

  public TransferController(TransferProducer transferProducer) {
    this.transferProducer = transferProducer;
  }

  /**
   * Sends a Task2Dto object. E.g. POST http://localhost:8080/api/v1/transfer/send
   *
   * <p>Spring Boot auto convert JSON to TransferDto
   */
  @PostMapping("/send")
  public ResponseEntity<String> sendTask1(@RequestBody TransferDto transferDto) {
    transferProducer.sendTransfer(transferDto);
    return ResponseEntity.ok("Sent to Kafka successfully");
  }

  /**
   * Sends a Task2Dto object to a specific topic. E.g. POST
   * http://localhost:8080/api/v1/transfer/some_topic
   *
   * <p>Spring Boot auto convert JSON to TransferDto
   */
  @PostMapping("/send/{topic}")
  public ResponseEntity<String> sendToSpecificTopic(
      @PathVariable String topic, @RequestBody TransferDto transferDto) {
    transferProducer.sendTransfer(topic, transferDto);
    return ResponseEntity.ok("Sent to topic: " + topic);
  }
}
