package com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.outbox.OutboxEvent;
import java.util.UUID;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Mapper(
    componentModel = "spring",
    imports = {UUID.class})
public abstract class OrderEntityOutboxEventMapping {
  @Value("${app.kafka.topics.task3}")
  protected String topic;

  @Autowired protected ObjectMapper objectMapper;

  @Mapping(target = "id", expression = "java(UUID.randomUUID())")
  @Mapping(target = "eventId", source = "id")
  @Mapping(target = "eventType", constant = "OrderCreated")
  @Mapping(target = "topic", expression = "java(topic)")
  @Mapping(target = "payload", expression = "java(mapPayload(orderEntity))")
  @Mapping(target = "sent", ignore = true)
  @Mapping(target = "sentAt", ignore = true)
  public abstract OutboxEvent toOutboxEvent(OrderEntity orderEntity);

  protected String mapPayload(OrderEntity orderEntity) {
    try {
      return objectMapper.writeValueAsString(orderEntity);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing OrderEntity to JSON", e);
    }
  }
}
