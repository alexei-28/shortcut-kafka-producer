package com.gmail.alexei28.shortcutkafkaproducer.task3.request;

import java.math.BigDecimal;

// DTO
public record CreateOrderRequest(String externalId, BigDecimal amount) {
  // for correct validate hashcode/equals
  public CreateOrderRequest {
    if (amount != null) {
      amount = amount.stripTrailingZeros();
    }
  }

  @Override
  public String toString() {
    return "\nCreateOrderRequest{"
        + "\nexternalId='"
        + externalId
        + '\''
        + ", \namount="
        + amount
        + "\n"
        + '}';
  }
}
