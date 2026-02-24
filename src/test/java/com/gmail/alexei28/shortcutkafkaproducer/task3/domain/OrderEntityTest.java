package com.gmail.alexei28.shortcutkafkaproducer.task3.domain;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Test;

class OrderEntityTest {
  @Test
  void simpleEqualsContract() {
    EqualsVerifier.forClass(OrderEntity.class)
        .withOnlyTheseFields("externalId")
        .suppress(Warning.IDENTICAL_COPY_FOR_VERSIONED_ENTITY)
        .verify();
  }
}
