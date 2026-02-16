package com.gmail.alexei28.shortcutkafkaproducer.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.gmail.alexei28.shortcutkafkaproducer.dto.Message;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@ActiveProfiles("test")
@SpringBootTest
@Testcontainers
class MessageProducerIT {
  @Value("${app.kafka.topics.message}")
  private String topic;

  private static final String CONSUMER_GROUP = "test-message-consumer-group";

  @Container @ServiceConnection
  static KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.1"));

  // Важно: PostgreSQLContainer должен быть объявлен после KafkaContainer,
  // так как Spring Boot может пытаться подключиться к БД до того, как Kafka будет готов.
  @Container @ServiceConnection
  static PostgreSQLContainer<?> postgresContainer =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

  @Autowired private MessageProducer messageProducer;
  private Message producedMessage;

  private static final Logger logger = LoggerFactory.getLogger(MessageProducerIT.class);

  @BeforeEach
  void setUp() {
    producedMessage =
        new Message(
            System.currentTimeMillis(),
            "MessageLog_Test_".concat(String.valueOf(System.currentTimeMillis())),
            LocalDateTime.now());
  }

  /*
    Проверка, что сообщение появляется в топике
    Message actually reached Kafka broker -> read it from topic.

    Если вы хотите проверить наличие сообщения в топике, не полагаясь на свой consumer,
    можно использовать KafkaTestUtils для создания временного потребителя прямо внутри теста,
    но подход со Spy на реальном consumer лучше проверяет всю цепочку интеграции.
    Этот подход хорош тем, что вы тестируете «чистую» отправку: попало ли сообщение в Kafka в принципе,
    независимо от того, работает ли ваш @KafkaListener.
    Это исключает ошибки в коде потребителя и проверяет только связку Producer -> Kafka.

    Интеграционный тест с использованием временного Consumer
    Независимость: Этот тест пройдет, даже если вы случайно удалите все @KafkaListener из вашего проекта.
  */
  @Test
  @DisplayName("Producer should send message to Kafka topic")
  void producerShouldSendMessageInKafkaTopic() {
    // Arrange
    // 1. Настраиваем временный Consumer вручную
    // KafkaTestUtils.consumerProps: Генерирует стандартную карту настроек для Consumer (адрес
    // брокера, десериализаторы, авто-коммит).
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps(kafkaContainer.getBootstrapServers(), CONSUMER_GROUP, "true");

    // DefaultKafkaConsumerFactory: Мы создаем фабрику вручную, чтобы точно указать десериализаторы.
    // В данном случае JsonDeserializer важен, если ваш Message улетает в JSON-формате.
    // Используем String для ключа и JsonDeserializer для тела (Message)
    DefaultKafkaConsumerFactory<String, Message> cf =
        new DefaultKafkaConsumerFactory<>(
            consumerProps, new StringDeserializer(), new JsonDeserializer<>(Message.class, false));
    Consumer<String, Message> consumer = cf.createConsumer();
    consumer.subscribe(Collections.singleton(topic));

    // Act
    // 2. Отправляем сообщение через наш сервис
    messageProducer.sendMessage(topic, producedMessage);
    // 3. Читаем запись напрямую из Kafka
    // KafkaTestUtils.getSingleRecord - блокирует выполнение теста и ждет появления ровно одной
    // записи в топике в течение таймаута. Если сообщение не придет — тест упадет с понятной
    // ошибкой.
    ConsumerRecord<String, Message> consumerRecord =
        KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(10));

    // 4. Assert
    Message actualMessage = consumerRecord.value();
    assertThat(actualMessage).isNotNull();
    assertThat(actualMessage.getContent()).isEqualTo(producedMessage.getContent());
    consumer.close();
  }
}
