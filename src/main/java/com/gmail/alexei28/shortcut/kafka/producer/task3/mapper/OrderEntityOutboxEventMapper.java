package com.gmail.alexei28.shortcut.kafka.producer.task3.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.enums.OutboxStatus;
import com.gmail.alexei28.shortcutkafkaproducer.task3.event.CreateOrderEvent;
import com.gmail.alexei28.shortcutkafkaproducer.task3.event.OutboxEvent;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Mapper(
    componentModel = "spring",
    imports = {UUID.class, OutboxStatus.class, OffsetDateTime.class})
public abstract class OrderEntityOutboxEventMapper {
  @Value("${app.kafka.topics.task3}")
  protected String topic;

  @Autowired protected ObjectMapper objectMapper;

  @Mapping(target = "id", expression = "java(UUID.randomUUID())")
  @Mapping(target = "aggregateType", constant = "Order")
  @Mapping(target = "aggregateId", source = "orderEntity.id")
  @Mapping(target = "topic", expression = "java(topic)")
  @Mapping(target = "createdAt", expression = "java(OffsetDateTime.now())")
  @Mapping(target = "payload", ignore = true) // Заполняем вручную в @AfterMapping
  @Mapping(target = "status", expression = "java(OutboxStatus.NEW)")
  @Mapping(target = "sentAt", ignore = true)
  public abstract OutboxEvent toOutboxEvent(
      OrderEntity orderEntity, CreateOrderEvent createOrderEvent);

  @AfterMapping
  protected void mapPayload(
      CreateOrderEvent createOrderEvent, @MappingTarget OutboxEvent outboxEvent) {
    try {
      // Превращаем объект события в JSON строку
      String json = objectMapper.writeValueAsString(createOrderEvent);
      outboxEvent.setPayload(json);
    } catch (Exception e) {
      throw new RuntimeException("Could not serialize CreateOrderEvent to JSON", e);
    }
  }
}
