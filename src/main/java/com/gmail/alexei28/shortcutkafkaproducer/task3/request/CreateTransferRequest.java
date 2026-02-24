package com.gmail.alexei28.shortcutkafkaproducer.task3.request;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record CreateTransferRequest(
    String senderIban,
    String receiverIban,
    BigDecimal amount,
    String status,
    String currency,
    OffsetDateTime createdAt) {
  public CreateTransferRequest {
    if (amount != null) {
      amount = amount.stripTrailingZeros();
    }
  }

  @Override
  public String toString() {
    return "\nCreateTransferRequest{"
        + "senderIban='"
        + senderIban
        + '\''
        + ", receiverIban='"
        + receiverIban
        + '\''
        + ", amount="
        + amount
        + ", status='"
        + status
        + '\''
        + ", currency='"
        + currency
        + '\''
        + ", createdAt="
        + createdAt
        + '}';
  }
}
