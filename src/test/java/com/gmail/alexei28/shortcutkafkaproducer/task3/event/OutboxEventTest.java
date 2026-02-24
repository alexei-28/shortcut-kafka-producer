package com.gmail.alexei28.shortcutkafkaproducer.task3.event;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Test;

class OutboxEventTest {
  @Test
  void simpleEqualsContract() {
    EqualsVerifier.forClass(OutboxEvent.class)
        .withOnlyTheseFields("aggregateId")
        .suppress(Warning.IDENTICAL_COPY_FOR_VERSIONED_ENTITY)
        .verify();
  }
}
