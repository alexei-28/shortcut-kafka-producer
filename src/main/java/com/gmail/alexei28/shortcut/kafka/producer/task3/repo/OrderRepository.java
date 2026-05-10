package com.gmail.alexei28.shortcut.kafka.producer.task3.repo;

import com.gmail.alexei28.shortcut.kafka.producer.task3.domain.OrderEntity;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {
  Optional<OrderEntity> findByExternalId(UUID externalId);
}
