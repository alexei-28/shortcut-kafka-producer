package com.gmail.alexei28.shortcutkafkaproducer.task3.dto;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(
    name = "outbox",
    indexes = {
      @Index(name = "idx_outbox_status", columnList = "status"),
      @Index(name = "idx_outbox_operation_id", columnList = "operation_id"),
      @Index(name = "idx_outbox_event_id", columnList = "event_id", unique = true)
    })
public class OutboxDto {
  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  private UUID id;

  // конкретное сообщение в Kafka (идентификатор сообщения Kafka) - идемпотентность сообщения
  // (Kafka-идемпотентность)
  @Column(name = "event_id", nullable = false, unique = true, updatable = false)
  private UUID eventId;

  // идемпотентность бизнес операции
  @Column(name = "operation_id", nullable = false, updatable = false)
  private UUID operationId;

  @Column(name = "sender_iban", nullable = false, length = 34)
  private String senderIban;

  @Column(name = "receiver_iban", nullable = false, length = 34)
  private String receiverIban;

  @Column(nullable = false, precision = 19, scale = 4)
  private BigDecimal amount;

  @Column(nullable = false, length = 3)
  private String currency;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private OutboxStatus status;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "published_at")
  private OffsetDateTime publishedAt;

  public UUID getId() {
    return id;
  }

  public UUID getEventId() {
    return eventId;
  }

  public void setEventId(UUID eventId) {
    this.eventId = eventId;
  }

  public UUID getOperationId() {
    return operationId;
  }

  public void setOperationId(UUID operationId) {
    this.operationId = operationId;
  }

  public String getSenderIban() {
    return senderIban;
  }

  public void setSenderIban(String senderIban) {
    this.senderIban = senderIban;
  }

  public String getReceiverIban() {
    return receiverIban;
  }

  public void setReceiverIban(String receiverIban) {
    this.receiverIban = receiverIban;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public String getCurrency() {
    return currency;
  }

  public void setCurrency(String currency) {
    this.currency = currency;
  }

  public OutboxStatus getStatus() {
    return status;
  }

  public void setStatus(OutboxStatus status) {
    this.status = status;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(OffsetDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public void markPublished() {
    this.status = OutboxStatus.PUBLISHED;
    this.publishedAt = OffsetDateTime.now();
  }

  public void markFailed() {
    this.status = OutboxStatus.FAILED;
  }

  @Override
  public String toString() {
    return "\nOutboxDto{"
        + "id = "
        + id
        + ", eventId = "
        + eventId
        + ", operationId = "
        + operationId
        + ", senderIban = '"
        + senderIban
        + '\''
        + ", receiverIban = '"
        + receiverIban
        + '\''
        + ", amount = "
        + amount
        + ", currency = '"
        + currency
        + '\''
        + ", status = "
        + status
        + ", createdAt = "
        + createdAt
        + ", publishedAt = "
        + publishedAt
        + '}';
  }
}
