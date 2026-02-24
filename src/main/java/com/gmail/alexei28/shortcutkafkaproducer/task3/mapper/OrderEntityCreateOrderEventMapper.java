package com.gmail.alexei28.shortcutkafkaproducer.task3.mapper;

import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.event.CreateOrderEvent;
import java.util.UUID;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(
    componentModel = "spring",
    imports = {UUID.class})
public interface OrderEntityCreateOrderEventMapper {
  @Mapping(target = "eventId", expression = "java(UUID.randomUUID())")
  @Mapping(target = "orderId", source = "id")
  CreateOrderEvent toOrderEvent(OrderEntity orderEntity);
}
