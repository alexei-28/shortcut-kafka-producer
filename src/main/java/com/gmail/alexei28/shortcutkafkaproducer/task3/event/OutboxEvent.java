package com.gmail.alexei28.shortcutkafkaproducer.task3.event;

import com.gmail.alexei28.shortcutkafkaproducer.task3.enums.OutboxStatus;
import com.gmail.alexei28.shortcutkafkaproducer.task3.util.JsonUtils;
import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(
    name = "outbox",
    indexes = @Index(name = "idx_outbox_status_created", columnList = "status, created_at"))
public class OutboxEvent {

  @Id private UUID id;

  @Column(name = "aggregate_type", nullable = false)
  private String aggregateType;

  @Column(name = "aggregate_id", nullable = false)
  private UUID aggregateId;

  @Column(nullable = false)
  private String topic;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "payload", nullable = false)
  private String payload;

  @Column(name = "created_at", nullable = false)
  private OffsetDateTime createdAt;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private OutboxStatus status;

  @Column(name = "sent_at")
  private OffsetDateTime sentAt;

  public OutboxEvent() {}

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getAggregateType() {
    return aggregateType;
  }

  public void setAggregateType(String aggregateType) {
    this.aggregateType = aggregateType;
  }

  public UUID getAggregateId() {
    return aggregateId;
  }

  public void setAggregateId(UUID aggregateId) {
    this.aggregateId = aggregateId;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(OffsetDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public OutboxStatus getStatus() {
    return status;
  }

  public void setStatus(OutboxStatus status) {
    this.status = status;
  }

  public OffsetDateTime getSentAt() {
    return sentAt;
  }

  public void setSentAt(OffsetDateTime sentAt) {
    this.sentAt = sentAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OutboxEvent that)) return false;
    return Objects.equals(aggregateId, that.aggregateId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(aggregateId);
  }

  @Override
  public String toString() {
    return "\nOutboxEvent{"
        + "\nid = "
        + id
        + ",\n aggregateType = '"
        + aggregateType
        + '\''
        + ",\n aggregateId = "
        + aggregateId
        + ",\n topic = '"
        + topic
        + '\''
        + ",\n payload = '"
        + JsonUtils.toPrettyJson(payload)
        + '\''
        + ",\n createdAt = "
        + createdAt
        + ",\n sent = "
        + status
        + ",\n sentAt = "
        + sentAt
        + '}';
  }
}
