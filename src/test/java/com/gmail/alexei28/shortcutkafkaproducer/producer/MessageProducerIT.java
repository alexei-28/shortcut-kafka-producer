package com.gmail.alexei28.shortcutkafkaproducer.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.StreamUtils;
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
  private static String jsonTemplate;
  private String producedValidJson;
  private long randomNumber;

  @BeforeAll
  static void beforeAll() throws IOException {
    jsonTemplate =
        StreamUtils.copyToString(
            new ClassPathResource("message_template.json").getInputStream(),
            StandardCharsets.UTF_8);
  }

  @BeforeEach
  void setUp() {
    randomNumber = new Random().nextLong(10000);
    // Update specific nodes in the JSON
    DocumentContext context =
        JsonPath.parse(jsonTemplate)
            .set("$.number", randomNumber)
            .set("$.content", "Message_TEST_PRODUCED_" + randomNumber);
    producedValidJson = context.jsonString();
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
    DefaultKafkaConsumerFactory<String, String> cf =
        new DefaultKafkaConsumerFactory<>(
            consumerProps, new StringDeserializer(), new JsonDeserializer<>(String.class, false));
    Consumer<String, String> consumer = cf.createConsumer();
    consumer.subscribe(Collections.singleton(topic));

    // Act
    // 2. Отправляем сообщение через наш сервис
    messageProducer.sendValue(topic, producedValidJson);
    // 3. Читаем запись напрямую из Kafka
    // KafkaTestUtils.getSingleRecord - блокирует выполнение теста и ждет появления ровно одной
    // записи в топике в течение таймаута. Если сообщение не придет — тест упадет с понятной
    // ошибкой.
    ConsumerRecord<String, String> consumerRecord =
        KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(10));

    // 4. Assert
    String actualMessage = consumerRecord.value();
    assertThat(actualMessage).isNotNull();
    assertThat(actualMessage).isEqualTo(producedValidJson);
    consumer.close();
  }
}
