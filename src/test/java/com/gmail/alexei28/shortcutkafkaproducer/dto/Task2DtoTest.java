package com.gmail.alexei28.shortcutkafkaproducer.dto;

import static org.junit.jupiter.api.Assertions.*;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class Task2DtoTest {
  @Test
  void simpleEqualsContract() {
    EqualsVerifier.simple().forClass(Task2Dto.class).verify();
  }
}
