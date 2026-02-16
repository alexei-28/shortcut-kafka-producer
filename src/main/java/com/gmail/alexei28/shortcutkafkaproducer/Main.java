package com.gmail.alexei28.shortcutkafkaproducer;

import com.gmail.alexei28.shortcutkafkaproducer.configuration.VersionInfoListener;
import com.gmail.alexei28.shortcutkafkaproducer.dto.Message;
import com.gmail.alexei28.shortcutkafkaproducer.producer.MessageProducer;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Main {
  private static ConfigurableApplicationContext context;
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(Main.class);
    app.addListeners(new VersionInfoListener());
    context = app.run(args);

    logger.info("Application started successfully!");
    logger.info(
        "Java version: {}, Java vendor: {}",
        System.getProperty("java.version"),
        System.getProperty("java.vendor"));

    // sendMessage();
  }

  private static void sendMessage() {
    MessageProducer messageProducer = context.getBean(MessageProducer.class);
    messageProducer.sendMessage(
        new Message(
            System.currentTimeMillis(),
            "MessageLog_Prod_".concat(String.valueOf(System.currentTimeMillis())),
            LocalDateTime.now()));
  }
}
