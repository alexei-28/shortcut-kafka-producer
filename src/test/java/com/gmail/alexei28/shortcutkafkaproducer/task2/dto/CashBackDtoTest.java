package com.gmail.alexei28.shortcutkafkaproducer.task2.dto;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class CashBackDtoTest {
  @Test
  void simpleEqualsContract() {
    EqualsVerifier.simple().forClass(CashbackDto.class).verify();
  }
}
