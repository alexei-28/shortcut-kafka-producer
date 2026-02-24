package com.gmail.alexei28.shortcutkafkaproducer.task3.idempotency;

import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.UUID;

// Защита REST. Таблица,которая спасает от double-click, retry nginx, retry mobile app.
// Сохраняем HTTP ответ. Это важно: при retry клиент получит тот же самый ответ.
@Entity
@Table(
    name = "idempotency_keys",
    indexes = {@Index(name = "idx_idemp_key", columnList = "idempotency_key", unique = true)})
public class IdempotencyKeyEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  private UUID id;

  @Column(name = "idempotency_key", nullable = false, unique = true, updatable = false)
  private String idempotencyKey;

  @Column(name = "response_code")
  private Integer responseCode;

  @Column(name = "response_body", columnDefinition = "TEXT")
  private String responseBody;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @PrePersist
  void prePersist() {
    createdAt = OffsetDateTime.now();
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getIdempotencyKey() {
    return idempotencyKey;
  }

  public void setIdempotencyKey(String idempotencyKey) {
    this.idempotencyKey = idempotencyKey;
  }

  public Integer getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(Integer responseCode) {
    this.responseCode = responseCode;
  }

  public String getResponseBody() {
    return responseBody;
  }

  public void setResponseBody(String responseBody) {
    this.responseBody = responseBody;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(OffsetDateTime createdAt) {
    this.createdAt = createdAt;
  }

  @Override
  public String toString() {
    return "\nIdempotencyKeyEntity{"
        + "\n id="
        + id
        + ",\n idempotencyKey='"
        + idempotencyKey
        + '\''
        + ",\n responseCode="
        + responseCode
        + ",\n responseBody='"
        + responseBody
        + '\''
        + ",\n createdAt="
        + createdAt
        + "\n"
        + '}';
  }
}
