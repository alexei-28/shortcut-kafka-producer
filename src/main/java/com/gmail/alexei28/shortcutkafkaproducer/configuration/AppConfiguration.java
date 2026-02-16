package com.gmail.alexei28.shortcutkafkaproducer.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

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
  @Value("${app.kafka.topics.message}")
  private String messageTopic;

  private static final Logger logger = LoggerFactory.getLogger(AppConfiguration.class);

  /*-
  @Bean
  public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template) {
    // 1. Определяем логику восстановления (отправка в .DLT топик)
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
    // 2. Настраиваем попытки (например, 3 ретрая через 2 секунды)
    // Для Poison Pill (ошибки десериализации) ретраи обычно пропускаются
    return new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3));
  }
   */

  /*
    Будет создан топик "message_topic" с 3 партициями, одна из которых будет лидером.
    1 leader partition + 2 followers partitions (2 резервные партиции).
    Резервные партиции синхронизируются с лидером.
    Чтобы защититься от потери данных и обеспечить высокую доступность, партиции реплицируют.
    Чаще всего используют фактор репликации 3 (RF=3), то есть на разных брокерах хранятся три копии каждой партиции.
    Если один брокер «выключился», его копия всё равно присутствует у других, так что обработка продолжается.
  */
  @Bean
  public NewTopic crateMessageTopic() {
    logger.info("crateTopic, topic '{}' with 1 partition and replicas 3", messageTopic);
    return TopicBuilder.name(messageTopic).partitions(1).replicas(3).build();
  }
}
