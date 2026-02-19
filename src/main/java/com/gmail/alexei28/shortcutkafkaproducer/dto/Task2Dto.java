package com.gmail.alexei28.shortcutkafkaproducer.dto;

import java.time.LocalDateTime;
import java.util.Objects;

public class Task2Dto {
  private Long number;
  private String content;
  private LocalDateTime receivedAt;

  public Task2Dto() {}

  public Long getNumber() {
    return number;
  }

  public void setNumber(Long number) {
    this.number = number;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public LocalDateTime getReceivedAt() {
    return receivedAt;
  }

  public void setReceivedAt(LocalDateTime receivedAt) {
    this.receivedAt = receivedAt;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    Task2Dto task2Dto = (Task2Dto) o;
    return Objects.equals(number, task2Dto.number)
        && Objects.equals(content, task2Dto.content)
        && Objects.equals(receivedAt, task2Dto.receivedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(number, content, receivedAt);
  }

  @Override
  public String toString() {
    return "Task2Dto{"
        + "number="
        + number
        + ", content='"
        + content
        + '\''
        + ", receivedAt="
        + receivedAt
        + '}';
  }
}
