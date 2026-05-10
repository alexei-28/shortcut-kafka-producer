package com.gmail.alexei28.shortcut.kafka.producer.task3.domain;

import com.gmail.alexei28.shortcut.kafka.producer.task3.enums.OrderStatus;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(
    name = "orders",
    indexes = @Index(name = "idx_order_external_id", columnList = "external_id", unique = true))
public class OrderEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  private UUID id;

  // идемпотентность REST
  @Column(name = "external_id", nullable = false, unique = true)
  private UUID externalId;

  @Column(nullable = false)
  private BigDecimal amount;

  @Column(name = "currency", nullable = false, length = 3)
  private String currency;

  @Column(name = "sender_iban", nullable = false, length = 34)
  private String senderIBAN;

  @Column(name = "receiver_iban", nullable = false, length = 34)
  private String receiverIBAN;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private OrderStatus status;

  @Column(nullable = false)
  private OffsetDateTime createdAt;

  public OrderEntity() {}

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public UUID getExternalId() {
    return externalId;
  }

  public void setExternalId(UUID externalId) {
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

  public String getCurrency() {
    return currency;
  }

  public void setCurrency(String currency) {
    this.currency = currency;
  }

  public String getSenderIBAN() {
    return senderIBAN;
  }

  public void setSenderIBAN(String senderIBAN) {
    this.senderIBAN = senderIBAN;
  }

  public String getReceiverIBAN() {
    return receiverIBAN;
  }

  public void setReceiverIBAN(String receiverIBAN) {
    this.receiverIBAN = receiverIBAN;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OrderEntity that)) return false;
    return Objects.equals(externalId, that.externalId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(externalId);
  }

  @Override
  public String toString() {
    return "\nOrder{"
        + "\nid = "
        + id
        + ",\n externalId = "
        + externalId
        + ",\n amount = "
        + amount
        + ",\n status = "
        + status
        + ",\n createdAt = "
        + createdAt
        + "\n"
        + '}';
  }
}
