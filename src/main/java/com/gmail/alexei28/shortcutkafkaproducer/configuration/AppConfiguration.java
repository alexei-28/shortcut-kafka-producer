package com.gmail.alexei28.shortcutkafkaproducer.configuration;

import com.gmail.alexei28.shortcutkafkaproducer.listeners.KafkaLoggingProducerListener;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Spring configuration class that defines Kafka topics for the application. It creates a topic
 * named "test_topic" with 3 partitions and the default replication factor (1).
 *
 * <p>If you want the bean to be available by default but disabled specifically for tests, your
 * logic must explicitly handle the "missing" state. The correct setup for "Always on, except in
 * tests"
 */
@Configuration
@Profile("!test") // Загружать только если профиль 'test' НЕ активен
public class AppConfiguration {
  @Value("${app.kafka.topics.task1}")
  private String task1Topic;

  @Value("${app.kafka.topics.task2}")
  private String task2Topic;

  private final KafkaTemplate<Object, Object> kafkaTemplate;
  private final KafkaLoggingProducerListener loggingListener;
  private static final Logger logger = LoggerFactory.getLogger(AppConfiguration.class);

  public AppConfiguration(
      KafkaTemplate<Object, Object> kafkaTemplate, KafkaLoggingProducerListener loggingListener) {
    this.kafkaTemplate = kafkaTemplate;
    this.loggingListener = loggingListener;
  }

  @PostConstruct
  public void registerListener() {
    // Устанавливаем наш слушатель в шаблон
    kafkaTemplate.setProducerListener(loggingListener);
  }

  /*
    Будет создан топик "message_topic" с 3 партициями, одна из которых будет лидером.
    1 leader partition + 2 followers partitions (2 резервные партиции).
    Резервные партиции синхронизируются с лидером.
    Чтобы защититься от потери данных и обеспечить высокую доступность, партиции реплицируют.
    Чаще всего используют фактор репликации 3 (RF=3), то есть на разных брокерах хранятся три копии каждой партиции.
    Если один брокер «выключился», его копия всё равно присутствует у других, так что обработка продолжается.
  */
  @Bean
  public NewTopic crateTask1Topic() {
    logger.info("crateTopic, topic '{}' with 1 partition and replicas 3", task1Topic);
    return TopicBuilder.name(task1Topic).partitions(1).replicas(3).build();
  }

  /*
    Будет создан топик "message_topic" с 3 партициями, одна из которых будет лидером.
    1 leader partition + 2 followers partitions (2 резервные партиции).
    Резервные партиции синхронизируются с лидером.
    Чтобы защититься от потери данных и обеспечить высокую доступность, партиции реплицируют.
    Чаще всего используют фактор репликации 3 (RF=3), то есть на разных брокерах хранятся три копии каждой партиции.
    Если один брокер «выключился», его копия всё равно присутствует у других, так что обработка продолжается.
  */
  @Bean
  public NewTopic crateTask2Topic() {
    logger.info("crateTopic, topic '{}' with 1 partition and replicas 3", task2Topic);
    return TopicBuilder.name(task2Topic).partitions(1).replicas(3).build();
  }
}
