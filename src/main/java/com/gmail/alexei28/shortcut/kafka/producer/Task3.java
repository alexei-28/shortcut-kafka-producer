package com.gmail.alexei28.shortcut.kafka.producer;

import com.gmail.alexei28.shortcutkafkaproducer.configuration.VersionInfoListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

// Validate is application is up: e.g. http://localhost:8081/api/v1/actuator/health
@SpringBootApplication
@EnableScheduling
public class Task3 {
  private static ConfigurableApplicationContext context;
  private static final Logger logger = LoggerFactory.getLogger(Task3.class);

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(Task3.class);
    app.addListeners(new VersionInfoListener());
    context = app.run(args);

    logger.info("\n\n ===== Application started successfully! =====\nЗадача 3 - Перевод через СБП");
    logger.info(
        "Java version: {}, Java vendor: {}",
        System.getProperty("java.version"),
        System.getProperty("java.vendor"));
  }
}
