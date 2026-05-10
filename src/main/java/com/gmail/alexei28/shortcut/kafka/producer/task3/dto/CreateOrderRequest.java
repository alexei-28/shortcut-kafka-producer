package com.gmail.alexei28.shortcut.kafka.producer.task3.dto;

import java.math.BigDecimal;
import java.util.UUID;

// DTO
public record CreateOrderRequest(
    UUID externalId, BigDecimal amount, String currency, String senderIBAN, String receiverIBAN) {
  // for correct validate hashcode/equals
  public CreateOrderRequest {
    if (amount != null) {
      amount = amount.stripTrailingZeros();
    }
  }

  @Override
  public String toString() {
    return "\nCreateOrderRequest{"
        + "\nexternalId = "
        + externalId
        + ",\n amount = "
        + amount
        + ",\n currency = "
        + currency
        + ",\n senderIBAN = "
        + senderIBAN
        + ",\n receiverIBAN = "
        + receiverIBAN
        + "\n"
        + '}';
  }
}
