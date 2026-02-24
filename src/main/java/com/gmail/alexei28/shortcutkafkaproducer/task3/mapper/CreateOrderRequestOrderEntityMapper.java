package com.gmail.alexei28.shortcutkafkaproducer.task3.mapper;

import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.CreateOrderRequest;
import com.gmail.alexei28.shortcutkafkaproducer.task3.enums.OrderStatus;
import java.time.OffsetDateTime;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(
    componentModel = "spring",
    imports = {OffsetDateTime.class, OrderStatus.class})
public interface CreateOrderRequestOrderEntityMapper {
  @Mapping(target = "id", ignore = true)
  @Mapping(target = "status", expression = "java(OrderStatus.NEW)")
  @Mapping(target = "createdAt", expression = "java(OffsetDateTime.now())")
  OrderEntity toEntity(CreateOrderRequest request);
}
