package com.gmail.alexei28.shortcutkafkaproducer.task3.command;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record CreateTransferCommand(
    UUID operationId,
    String senderIban,
    String receiverIban,
    BigDecimal amount,
    String currency,
    String status,
    OffsetDateTime createdAt) {
  public CreateTransferCommand {
    if (amount != null) {
      amount = amount.stripTrailingZeros();
    }
  }

  @Override
  public String toString() {
    return "CreateTransferCommand{"
        + "operationId="
        + operationId
        + ", senderIban='"
        + senderIban
        + '\''
        + ", receiverIban='"
        + receiverIban
        + '\''
        + ", amount="
        + amount
        + ", currency='"
        + currency
        + '\''
        + ", status='"
        + status
        + '\''
        + ", createdAt="
        + createdAt
        + '}';
  }
}
