package com.gmail.alexei28.shortcutkafkaproducer.task3.repository;

import com.gmail.alexei28.shortcutkafkaproducer.task3.outbox.OutboxEvent;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/*
  FOR UPDATE SKIP LOCKED — это механизм блокировки строк в СУБД,
  который используется для параллельной безопасной обработки записей, например в Transactional Outbox.
  Он означает:
    Заблокировать выбранные строки для обновления.
    Если строки уже заблокированы — просто пропустить их, а не ждать.

  Т.е.
  - если строка уже заблокирована другой транзакцией
  - мы её НЕ ждём
  - мы её ПРОПУСКАЕМ
  - берём следующую

  Пример:
  Допустим у тебя 3 инстанса сервиса:
  -Instance A
  -Instance B
  -Instance C

  С SKIP LOCKED:
    -A взял первые 50
    -B взял следующие 50
    -C взял следующие 50
  И все работают параллельно.
*/
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {
  List<OutboxEvent> findTop100BySentFalseOrderByCreatedAt();

  @Query(
      value =
          """
            SELECT *
            FROM outbox
            WHERE sent = false
            ORDER BY created_at
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
        """,
      nativeQuery = true)
  List<OutboxEvent> lockNextBatch(@Param("batchSize") int batchSize);
}
