package com.gmail.alexei28.shortcutkafkaproducer.task3.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(
    name = "orders",
    indexes = {
      @Index(name = "idx_orders_external_id", columnList = "external_id", unique = true),
      @Index(name = "idx_orders_status", columnList = "status")
    })
public class OrderEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  private UUID id;

  // Внешний id клиента (например orderId от frontend).Бизнес идемпотентность (если фронт ретраит ->
  // заказ не создастся дважды)
  @Column(name = "external_id", nullable = false, unique = true, updatable = false)
  private String externalId;

  @Column(nullable = false)
  private BigDecimal amount;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private OrderStatus status;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  public UUID getId() {
    return id;
  }

  public String getExternalId() {
    return externalId;
  }

  public void setExternalId(String externalId) {
    this.externalId = externalId;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public OrderStatus getStatus() {
    return status;
  }

  public void setStatus(OrderStatus status) {
    this.status = status;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(OffsetDateTime createdAt) {
    this.createdAt = createdAt;
  }

  @Override
  public String toString() {
    return "\nOrderEntity{"
        + "\nid="
        + id
        + ",\n externalId='"
        + externalId
        + '\''
        + ",\n amount="
        + amount
        + ",\n status="
        + status
        + ",\n createdAt="
        + createdAt
        + "\n"
        + '}';
  }
}
