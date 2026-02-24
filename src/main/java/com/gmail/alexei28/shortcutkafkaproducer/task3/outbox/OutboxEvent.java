package com.gmail.alexei28.shortcutkafkaproducer.task3.outbox;

import com.gmail.alexei28.shortcutkafkaproducer.task3.util.JsonUtils;
import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(
    name = "outbox",
    indexes = {@Index(name = "idx_outbox_sent", columnList = "sent, created_at")})
public class OutboxEvent {

  @Id private UUID id;

  @Column(name = "event_id", nullable = false)
  private UUID eventId;

  @Column(name = "event_type", nullable = false)
  private String eventType;

  @Column(name = "topic", nullable = false)
  private String topic;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "payload", nullable = false)
  private String payload;

  @Column(name = "sent", nullable = false)
  private boolean sent = false;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "sent_at")
  private OffsetDateTime sentAt;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public UUID getEventId() {
    return eventId;
  }

  public void setEventId(UUID eventId) {
    this.eventId = eventId;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
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

  public boolean isSent() {
    return sent;
  }

  public void setSent(boolean sent) {
    this.sent = sent;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(OffsetDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public OffsetDateTime getSentAt() {
    return sentAt;
  }

  public void setSentAt(OffsetDateTime sentAt) {
    this.sentAt = sentAt;
  }

  @Override
  public String toString() {
    return "\nOutboxEvent{"
        + "\n id = "
        + id
        + ",\n eventId = "
        + eventId
        + ",\n eventType = '"
        + eventType
        + '\''
        + ",\n topic = '"
        + topic
        + '\''
        + ",\n payload = '"
        + JsonUtils.toPrettyJson(this.payload)
        + '\''
        + ",\n sent = "
        + sent
        + ",\n createdAt = "
        + createdAt
        + ",\n sentAt = "
        + sentAt
        + "\n"
        + '}';
  }
}
