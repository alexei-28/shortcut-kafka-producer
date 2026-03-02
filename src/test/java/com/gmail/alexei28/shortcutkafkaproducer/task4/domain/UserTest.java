package com.gmail.alexei28.shortcutkafkaproducer.task4.domain;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class UserTest {
  @Test
  void simpleEqualsContract() {
    EqualsVerifier.simple().forClass(User.class).verify();
  }
}
