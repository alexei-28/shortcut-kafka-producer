package com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces;

import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderStatus;
import com.gmail.alexei28.shortcutkafkaproducer.task3.request.CreateOrderRequest;
import java.time.OffsetDateTime;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(
    componentModel = "spring",
    imports = {OffsetDateTime.class, OrderStatus.class})
public interface RequestOrderEntityMapping {
  @Mapping(target = "status", expression = "java(OrderStatus.NEW)")
  @Mapping(target = "createdAt", expression = "java(OffsetDateTime.now())")
  OrderEntity toEntity(CreateOrderRequest request);
}
