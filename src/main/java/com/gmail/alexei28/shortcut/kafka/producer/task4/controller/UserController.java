package com.gmail.alexei28.shortcut.kafka.producer.task4.controller;

import com.gmail.alexei28.shortcut.kafka.producer.task4.producer.UserProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/user")
public class UserController {
  private final UserProducer userProducer;
  private static final Logger logger = LoggerFactory.getLogger(UserController.class);

  public UserController(UserProducer userProducer) {
    this.userProducer = userProducer;
  }

  /**
   * Sends a Task2Dto object. E.g. POST http://localhost:8080/api/v1/user/create
   *
   * <p>Spring Boot auto convert JSON to Task2Dto
   */
  @PostMapping("/create")
  public String sendMessage(@RequestBody String userJson) {
    // Отправляем сырую JSON строку
    userProducer.sendRaw(userJson);
    return "Message sent to Kafka!";
  }
}
