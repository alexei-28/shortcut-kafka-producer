package com.gmail.alexei28.shortcutkafkaproducer.task3.repository;

import com.gmail.alexei28.shortcutkafkaproducer.task3.idempotency.IdempotencyKeyEntity;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface IdempotencyRepository extends JpaRepository<IdempotencyKeyEntity, UUID> {
  Optional<IdempotencyKeyEntity> findByIdempotencyKey(String key);
}
