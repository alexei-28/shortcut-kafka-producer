package com.gmail.alexei28.shortcutkafkaproducer.task3.service;

import com.gmail.alexei28.shortcutkafkaproducer.task3.outbox.OutboxEvent;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repository.OutboxRepository;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/*
  -Почему  работает @Transactional?
   Spring AOP работает только когда:
    Кто-то вызывает Spring bean через proxy

   Но НЕ работает когда:
     this.publishBatch()
   @Transactional не работает из-за self-invocation внутри того же класса.

  -Почему не нужно вручную обновлять БД после изменения outboxEvent.setSentAt()?
   Потому что в коде OutboxEvent находится в состоянии managed entity внутри транзакции,
   и Hibernate делает UPDATE автоматически через dirty checking.
   1. Что происходит при outboxRepository.lockNextBatch
     Несмотря на то что это nativeQuery = true, Hibernate:
     -выполняет SELECT
     -мапит строки в OutboxEvent
     -помещает их в Persistence Context
     -начинает их отслеживать

    То есть объекты становятся: managed
    2. Что происходит при изменении полей
        outboxEvent.setSent(true);
        outboxEvent.setSentAt(OffsetDateTime.now());
     Hibernate:
     - не делает UPDATE сразу
     - помечает entity как dirty

    3. Что происходит при выходе из метода publishBatch?
      Метод publishBatch помечен @Transactional
      При завершении метода publishBatch происходит:
        flush()
        commit
*/
@Service
public class OutboxTransactionalService {
  private final OutboxRepository outboxRepository;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private static final int BATCH_SIZE = 50;
  private static final Logger logger = LoggerFactory.getLogger(OutboxTransactionalService.class);

  public OutboxTransactionalService(
      OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate) {
    this.outboxRepository = outboxRepository;
    this.kafkaTemplate = kafkaTemplate;
  }

  @Transactional
  public void publishOutboxEvent() {
    // 1. захватываем события
    List<OutboxEvent> outboxEventList = outboxRepository.lockNextBatch(BATCH_SIZE);
    if (outboxEventList.isEmpty()) {
      return;
    }
    logger.info("publishOutboxEvent, publishing {} outboxEvent", outboxEventList.size());

    // 2. отправляем в Kafka
    for (OutboxEvent outboxEvent : outboxEventList) {
      try {
        CompletableFuture<SendResult<String, String>> completableFuture =
            kafkaTemplate.send(
                outboxEvent.getTopic(),
                outboxEvent.getEventId().toString(), // key
                outboxEvent.getPayload());

        completableFuture.get(); // синхронно - ждем ack ОТ Kafka
        // Порядок: Kafka записал -> commit DB

        // 3. помечаем отправленным
        outboxEvent.setSent(true);
        outboxEvent.setSentAt(OffsetDateTime.now());
        logger.info("publishOutboxEvent, successfully sent to Kafka outboxEvent = {}", outboxEvent);
      } catch (Exception e) {
        // НИЧЕГО НЕ ДЕЛАЕМ
        // Транзакция откатится -> событие будет отправлено снова
        logger.error("publishOutboxEvent, Kafka send failed for event {}", outboxEvent.getId(), e);
        throw new RuntimeException(e);
      }
    }
  }
}
