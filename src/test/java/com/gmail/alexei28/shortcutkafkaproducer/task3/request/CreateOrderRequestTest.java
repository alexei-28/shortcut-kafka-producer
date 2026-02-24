package com.gmail.alexei28.shortcutkafkaproducer.task3.request;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class CreateOrderRequestTest {
  @Test
  void simpleEqualsContract() {
    EqualsVerifier.simple().forClass(CreateOrderRequest.class).verify();
  }
}
