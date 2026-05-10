package com.gmail.alexei28.shortcut.kafka.producer.task3.dto;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class CreateOrderRequestTest {
  @Test
  void simpleEqualsContract() {
    EqualsVerifier.simple().forClass(CreateOrderRequest.class).verify();
  }
}
