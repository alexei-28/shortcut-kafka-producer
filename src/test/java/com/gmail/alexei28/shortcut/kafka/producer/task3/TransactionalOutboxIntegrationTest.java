package com.gmail.alexei28.shortcut.kafka.producer.task3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gmail.alexei28.shortcut.kafka.producer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcut.kafka.producer.task3.dto.CreateOrderRequest;
import com.gmail.alexei28.shortcut.kafka.producer.task3.enums.OutboxStatus;
import com.gmail.alexei28.shortcut.kafka.producer.task3.event.OutboxEvent;
import com.gmail.alexei28.shortcut.kafka.producer.task3.producer.OutboxProducer;
import com.gmail.alexei28.shortcut.kafka.producer.task3.repo.OrderRepository;
import com.gmail.alexei28.shortcut.kafka.producer.task3.repo.OutboxEventRepository;
import com.gmail.alexei28.shortcut.kafka.producer.task3.service.OrderService;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import jakarta.persistence.EntityManager;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.util.StreamUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/*
   Для полноценного тестирования стратегии Transactional Outbox нам нужно проверить три ключевых аспекта:
   1. Атомарность: Запись в таблицу бизнес-логики (Order) и таблицу Outbox происходит в одной транзакции.
   2. Гарантия доставки (At-least-once): Событие из Outbox попадает в Kafka, после чего статус в БД меняется на SENT.
   3. Отказоустойчивость: Если Kafka недоступна, транзакция в БД не должна помечать событие как отправленное,
       чтобы при следующем запуске планировщика произошла повторная попытка.
*/
@ActiveProfiles("test")
@SpringBootTest
@Testcontainers
class TransactionalOutboxIntegrationTest {
  @Value("${app.kafka.topics.task3}")
  private String expectedTopic;

  @Container @ServiceConnection
  static KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.1"));

  // Важно: PostgreSQLContainer должен быть объявлен после KafkaContainer,
  // так как Spring Boot может пытаться подключиться к БД до того, как Kafka будет готов.
  @Container @ServiceConnection
  static PostgreSQLContainer<?> postgresContainer =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

  @Autowired private EntityManager entityManager;
  @Autowired private OrderService orderService;
  @Autowired private OrderRepository orderRepository;
  @Autowired private OutboxEventRepository outboxEventRepository;
  @Autowired private OutboxProducer outboxProducer;
  /*
      Поскольку KafkaTemplate помечен как @MockitoSpyBean, Spring использует реальный экземпляр,
      но позволяет нам «подсматривать» за его методами через verify.
      @MockitoSpyBean: Позволяет нам следить за реальным бином KafkaTemplate.
      Spring создает настоящий экземпляр вашего KafkaTemplate со всеми его зависимостями (repository, taskMapper).
      Обертка (Spy): Mockito «оборачивает» этот реальный объект.
      Это позволяет вам:
      -Вызывать реальные методы (код внутри consume и process будет выполнен).
      -Следить за вызовами (использовать verify, чтобы посчитать количество вызовов).
      -Переопределять поведение только конкретных методов, если нужно (через doThrow или doReturn).
  */
  @MockitoSpyBean private KafkaTemplate<?, ?> kafkaTemplateSpy;
  @Autowired private ObjectMapper objectMapper;
  private static String jsonTemplate;
  private CreateOrderRequest createOrderRequest;

  @BeforeAll
  static void beforeAll() throws IOException {
    jsonTemplate =
        StreamUtils.copyToString(
            new ClassPathResource("create_order_template.json").getInputStream(),
            StandardCharsets.UTF_8);
  }

  @BeforeEach
  void setUp() throws JsonProcessingException {
    outboxEventRepository.deleteAll();
    orderRepository.deleteAll();
    entityManager.clear();

    // Update specific nodes in the JSON
    DocumentContext context =
        JsonPath.parse(jsonTemplate).set("$.externalId", UUID.randomUUID().toString());
    String createOrderRequestValidJson = context.jsonString();
    createOrderRequest =
        objectMapper.readValue(createOrderRequestValidJson, CreateOrderRequest.class);
  }

  /*
  Тест проверяет:
    -атомарность записи Order + Outbox
    -изменение статуса после ACK
    -факт вызова KafkaTemplate.send
    -работу @Scheduled
   */
  @Test
  @DisplayName("Успешный цикл: создание заказа -> запись в Outbox -> отправка в Kafka")
  void shouldCreateOrderAndSendMessageToKafkaViaOutbox() {
    // Act
    // 1. Создаем заказ через сервис
    OrderEntity expectedOrderEntity = orderService.createOrder(createOrderRequest);

    // Assert
    // 2. Проверяем, что в БД созданы записи (Атомарность)
    List<OrderEntity> orderEntityList = orderRepository.findAll();
    assertThat(orderEntityList).hasSize(1);
    OrderEntity actualOrderEntity = orderEntityList.getFirst();
    assertThat(actualOrderEntity.getId()).isEqualTo(expectedOrderEntity.getId());
    assertThat(actualOrderEntity.getStatus()).isEqualTo(expectedOrderEntity.getStatus());

    List<OutboxEvent> outboxEventList = outboxEventRepository.findAll();
    assertThat(outboxEventList).hasSize(1);
    OutboxEvent actualOutboxEvent = outboxEventList.getFirst();
    assertThat(actualOutboxEvent.getStatus()).isEqualTo(OutboxStatus.NEW);
    assertThat(actualOutboxEvent.getTopic()).isEqualTo(expectedTopic);
    assertThat(actualOutboxEvent.getSentAt()).isNull();

    // 3. Дожидаемся работы планировщика OutboxProducer.
    // Используем Awaitility, так как publishOutboxEvent работает по расписанию.
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              OutboxEvent updatedOutboxEvent =
                  outboxEventRepository.findById(actualOutboxEvent.getId()).orElseThrow();
              assertThat(updatedOutboxEvent.getStatus()).isEqualTo(OutboxStatus.SENT);
              assertThat(updatedOutboxEvent.getSentAt()).isNotNull();
            });

    // 4. Проверяем, что сообщение реально дошло до Kafka
    verify(kafkaTemplateSpy, times(1)).send(eq(expectedTopic), any(), any());
  }

  /*
    Доставка (Happy Path): Публикует ли планировщик событие в Kafka и обновляет ли статус в БД.
  */
  @DisplayName("Should publish event to Kafka and update outbox status to SENT")
  void shouldPublishToKafkaAndStatusUpdate() {
    // 1. Создаем заказ (событие в статусе NEW)
    orderService.createOrder(createOrderRequest);

    // 2. Вручную вызываем продьюсер (или ждем @Schedule)
    outboxProducer.publishOutboxEvent();

    // 3. Проверяем вызов KafkaTemplate
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(() -> verify(kafkaTemplateSpy).send(eq(expectedTopic), any(), any()));

    // 4. Проверяем обновление статуса в БД
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              List<OutboxEvent> events = outboxEventRepository.findAll();
              assertThat(events).hasSize(1);
              assertThat(events.getFirst().getStatus()).isEqualTo(OutboxStatus.SENT);
              assertThat(events.getFirst().getSentAt()).isNotNull();
            });
  }

  /*
    Гарантия At-least-once: Что происходит, если Kafka временно недоступна (откат транзакции и повторная попытка).
  */
  @Test
  @DisplayName("Should retry sending if Kafka fails (At-least-once)")
  void shouldRetrySendingIfKafkaFails() {
    // 1. Имитируем ошибку Kafka при первой попытке
    doReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka Down")))
        .doCallRealMethod() // Вторая попытка успешна
        .when(kafkaTemplateSpy)
        .send(any(), any(), any());

    // 2. Создаем заказ
    orderService.createOrder(createOrderRequest);

    // 3. Первая попытка публикации (должна выбросить исключение и откатить транзакцию)
    try {
      outboxProducer.publishOutboxEvent();
    } catch (Exception e) {
      // Ожидаем ошибку
    }

    // 4. Проверяем, что статус всё еще NEW (транзакция откатилась)
    assertThat(outboxEventRepository.findAll().getFirst().getStatus()).isEqualTo(OutboxStatus.NEW);

    // 5. Вторая попытка публикации (успешная)
    outboxProducer.publishOutboxEvent();

    // 6. Проверяем итог
    List<OutboxEvent> events = outboxEventRepository.findAll();
    assertThat(events.getFirst().getStatus()).isEqualTo(OutboxStatus.SENT);
    verify(kafkaTemplateSpy, times(2)).send(any(), any(), any());
  }

  /*
       Тест проверяет:
       - send() кидает exception
       - @Transactional откатывает изменения
       - статус OutboxEvent не стал SENT (остался NEW)
  */
  @Test
  @DisplayName(
      "Если Kafka недоступна -> транзакция откатывается -> статус OutboxEvent остается NEW")
  void shouldRollbackTransactionIfKafkaFails() {

    orderService.createOrder(createOrderRequest);

    OutboxEvent event = outboxEventRepository.findAll().getFirst();

    doReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka down")))
        .when(kafkaTemplateSpy)
        .send(any(), any(), any());

    assertThatThrownBy(() -> outboxProducer.publishOutboxEvent())
        .isInstanceOf(RuntimeException.class);

    entityManager.clear(); // ВАЖНО

    OutboxEvent reloaded = outboxEventRepository.findById(event.getId()).orElseThrow();

    assertThat(reloaded.getStatus()).isEqualTo(OutboxStatus.NEW);
    assertThat(reloaded.getSentAt()).isNull();
  }

  /*
    Проверка идемпотентности запроса (externalId)
  */
  @Test
  @DisplayName("Повторный externalId не создает второй заказ и второй outbox event")
  void shouldNotCreateDuplicateOrderAndOutboxEvent() {
    // Act
    orderService.createOrder(createOrderRequest);

    // Assert
    assertThatThrownBy(() -> orderService.createOrder(createOrderRequest))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Duplicate request");
    assertThat(orderRepository.count()).isEqualTo(1);
    assertThat(outboxEventRepository.count()).isEqualTo(1);
  }

  /*
  Проверка SKIP LOCKED (конкурентная обработка)
   */
  @Test
  @DisplayName("lockNextBatch использует FOR UPDATE SKIP LOCKED")
  void shouldNotProcessSameEventTwiceConcurrently() throws Exception {
    // Act
    orderService.createOrder(createOrderRequest);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<?> f1 = executor.submit(() -> outboxProducer.publishOutboxEvent());
    Future<?> f2 = executor.submit(() -> outboxProducer.publishOutboxEvent());
    f1.get();
    f2.get();

    OutboxEvent outboxEvent = outboxEventRepository.findAll().getFirst();

    // Assert
    assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.SENT);
    assertThat(outboxEvent.getSentAt()).isNotNull();
    verify(kafkaTemplateSpy, times(1)).send(eq(expectedTopic), any(), any());
  }
}
