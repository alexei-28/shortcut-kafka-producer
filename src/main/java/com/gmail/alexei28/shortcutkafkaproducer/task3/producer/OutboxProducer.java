package com.gmail.alexei28.shortcutkafkaproducer.task3.producer;

import com.gmail.alexei28.shortcutkafkaproducer.task3.enums.OutboxStatus;
import com.gmail.alexei28.shortcutkafkaproducer.task3.event.OutboxEvent;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repo.OutboxEventRepository;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/*

    Задача 3 — «Перевод через СБП»
    Сервис обрабатывает межбанковские переводы. Клиент инициировал перевод → списание со счёта → результат в следующий сервис по цепочке.

    Что говорит бизнес:

    Двойное списание — P1, штраф от регулятора
    Потеря перевода — P1, клиент без денег
    Нужен полный аудит каждой операции
    Стек: PostgreSQL + Kafka
    Задание: реализуй полный цикл обработки. Ни потеря, ни дубль недопустимы — выбери подход и обоснуй.

  - Решение
    Transactional Outbox (at-least-once) + Idempotent Consumer (exactly-once на бизнес-уровне)
    -на стороне producer-a
     - Гарантия at-least-once
    -на стороне consumer-a
     - Гарантия at-least-once на уровне инфраструктуры(Kafka) и exactly-once на уровне бизнес-логики.

    На стороне producer реализуем стратегию at-least-once.
    Transactional Outbox гарантирует:
    - НЕТ потери
    - НО возможны дубли
    Это не проблема, если правильно настроен consumer.
    Consumer + processed_event
    то:
    - дубликат безопасен
    - consumer проигнорирует повтор
    Это и есть at-least-once + consumer idempotency.
    Правило: При использовании Transactional Outbox, то consumer обязательно должен быть идемпотентным.
    Он должен проверять, обрабатывал ли он уже сообщение с таким eventId или aggregateId + version.

    Идемпотентный Producer в Kafka
    В настройках Kafka Producer:
        enable.idempotence=true
    Это защитит от дублей, если сбой произошел внутри самой Kafka при репликации между брокерами,
    но не спасет от логического дубля при откате транзакции в БД.

  - Архитектура:
    REST -> Service (@Transactional)
              ├─ сохраняем OrderEntity
              └─ сохраняем OutboxEvent (в той же транзакции)

    OutboxPublisher (scheduler)
              └─ читает NEW события → отправляет в Kafka → помечает SENT

    KafkaConsumer
              └─ идемпотентная обработка (через processed_event)

    At-least-once гарантия:
    Если:
    -Kafka отправила
    -но приложение упало ДО commit DB

    то:
    -статус не обновится (статус останется OutboxStatus.NEW)
    -событие будет отправлено повторно

    Это и есть at-least-once
*/
@Component
public class OutboxProducer {
  private final OutboxEventRepository outboxEventRepository;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private static final Logger logger = LoggerFactory.getLogger(OutboxProducer.class);

  public OutboxProducer(
      OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
    this.outboxEventRepository = outboxEventRepository;
    this.kafkaTemplate = kafkaTemplate;
  }

  /*
   Схема:
      SELECT ... FOR UPDATE (lockNextBatch)
      → send to Kafka
      → wait ACK
      → update status = SENT
      → commit

    Это textbook Outbox Pattern.
  */
  @Scheduled(fixedDelay = 500)
  @Transactional
  public void publishOutboxEvent() {
    logger.debug(
        "publishOutboxEvent, isActualTransactionActive = {}",
        TransactionSynchronizationManager.isActualTransactionActive());
    // 1. захватываем события (FOR UPDATE SKIP LOCKED)
    List<OutboxEvent> outboxEventList = outboxEventRepository.lockNextBatch(50);
    if (outboxEventList.isEmpty()) {
      return;
    }

    // 2. Отправляем в Kafka (at-least-once)
    // Схема: Kafka ACK -> update status -> commit DB
    logger.info("publishOutboxEvent, publishing {} outboxEvent", outboxEventList.size());
    for (OutboxEvent outboxEvent : outboxEventList) {
      try {
        // Отправляем в Kafka
        CompletableFuture<SendResult<String, String>> completableFuture =
            kafkaTemplate.send(
                outboxEvent.getTopic(),
                outboxEvent.getAggregateId().toString(), // key
                outboxEvent.getPayload());

        completableFuture.get(); // синхронно - ждем ack ОТ Kafka
        // Порядок: Kafka записал -> commit DB

        // 3. помечаем отправленным
        outboxEvent.setStatus(OutboxStatus.SENT);
        outboxEvent.setSentAt(OffsetDateTime.now());
        logger.info("publishOutboxEvent, successfully sent to Kafka outboxEvent = {}", outboxEvent);
      } catch (Exception e) {
        // НИЧЕГО НЕ ДЕЛАЕМ
        // Транзакция откатится -> событие будет отправлено снова
        logger.error("publishOutboxEvent, failed send event to Kafka -> rollback transaction!", e);
        throw new RuntimeException(e);
      }
    }
  }
}
