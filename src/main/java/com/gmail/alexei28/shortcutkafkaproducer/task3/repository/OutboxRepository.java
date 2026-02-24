package com.gmail.alexei28.shortcutkafkaproducer.task3.repository;

import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.OutboxDto;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxDto, UUID> {
  @Query(
      value =
          """
            SELECT *
            FROM outbox
            WHERE status IN ('NEW','FAILED')
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT :batchSize
            """,
      nativeQuery = true)
  List<OutboxDto> lockBatchForPublish(@Param("batchSize") int batchSize);
}
