package com.gmail.alexei28.shortcutkafkaproducer;

import com.gmail.alexei28.shortcutkafkaproducer.configuration.VersionInfoListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

// Validate is application is up: http://localhost:8081/api/v1/actuator/health
@SpringBootApplication
public class Main {
  private static ConfigurableApplicationContext context;
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(Main.class);
    app.addListeners(new VersionInfoListener());
    context = app.run(args);
    logger.info(
        "\n\n ===== Application started successfully! =====\n Задача 4 - Битые сообщения из CFT");
    logger.info(
        "Java version: {}, Java vendor: {}",
        System.getProperty("java.version"),
        System.getProperty("java.vendor"));
  }
}
