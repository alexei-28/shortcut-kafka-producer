package com.gmail.alexei28.shortcut.kafka.producer.task3.repo;

import com.gmail.alexei28.shortcut.kafka.producer.task3.event.OutboxEvent;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/*
   1. FOR UPDATE (Эксклюзивная блокировка)
       Без этой фразы несколько экземпляров вашего приложения (продьюсеров) могут одновременно выполнить SELECT,
       получить одни и те же ID событий и попытаться отправить их в Kafka.
       FOR UPDATE вешает эксклюзивную блокировку (X-lock) на выбранные строки.
       Другие транзакции, пытающиеся прочитать эти же строки с FOR UPDATE, будут ждать, пока первая транзакция не сделает COMMIT или ROLLBACK.
   2. SKIP LOCKED (Пропуск заблокированных строк)
       База данных видит, что первые batchSize строк уже кем-то заблокированы, и вместо того, чтобы ждать,
       она просто пропускает их и выдает второму сервису следующие batchSize доступных строк.

*/
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
  @Query(
      value =
          """
          SELECT * FROM outbox e
          WHERE status = 'NEW'
          ORDER BY created_at
          LIMIT :batchSize
          FOR UPDATE SKIP LOCKED
          """,
      nativeQuery = true)
  List<OutboxEvent> lockNextBatch(@Param("batchSize") int batchSize);
}
