package com.gmail.alexei28.shortcutkafkaproducer.task3.response;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class CreateOrderResponseTest {
  @Test
  void simpleEqualsContract() {
    EqualsVerifier.simple().forClass(CreateOrderResponse.class).verify();
  }
}
