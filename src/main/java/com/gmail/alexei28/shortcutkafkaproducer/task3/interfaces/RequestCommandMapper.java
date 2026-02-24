package com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces;

import com.gmail.alexei28.shortcutkafkaproducer.task3.command.CreateTransferCommand;
import com.gmail.alexei28.shortcutkafkaproducer.task3.request.CreateTransferRequest;
import java.util.UUID;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

// Чтобы маппер стал Spring-бином
@Mapper(componentModel = "spring")
public interface RequestCommandMapper {
  @Mapping(target = "operationId", source = "idempotencyKey")
  CreateTransferCommand toCommand(CreateTransferRequest request, UUID idempotencyKey);
}
