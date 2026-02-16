package com.gmail.alexei28.shortcutkafkaproducer.controller;

import com.gmail.alexei28.shortcutkafkaproducer.entity.Message;
import com.gmail.alexei28.shortcutkafkaproducer.producer.MessageProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/messages")
public class MessageController {
  private final MessageProducer messageProducer;

  public MessageController(MessageProducer messageProducer) {
    this.messageProducer = messageProducer;
  }

  /** Sends a Message object. E.g. POST http://localhost:8080/api/v1/messages/send */
  @PostMapping("/send")
  public ResponseEntity<String> sendMessage(@RequestBody Message message) {
    messageProducer.sendMessage(message);
    return ResponseEntity.ok("Message sent to Kafka successfully");
  }

  /**
   * Sends a message to a specific topic. E.g. POST
   * http://localhost:8080/api/v1/messages/send/message-topic
   */
  @PostMapping("/send/{topic}")
  public ResponseEntity<String> sendToSpecificTopic(
      @PathVariable String topic, @RequestBody Message message) {
    messageProducer.sendMessage(topic, message);
    return ResponseEntity.ok("Message sent to topic: " + topic);
  }

  /**
   * Sends a any string value (e.g., valid/invalid JSON). E.g. POST
   * http://localhost:8080/api/v1/messages/send-value
   */
  @PostMapping("/send-value")
  public ResponseEntity<String> sendRawValue(@RequestBody String value) {
    messageProducer.sendValue(value);
    return ResponseEntity.ok("Raw value sent to Kafka successfully");
  }
}
