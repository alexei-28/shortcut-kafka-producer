package com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces;

import com.gmail.alexei28.shortcutkafkaproducer.task3.command.CreateTransferCommand;
import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.Transfer;
import org.mapstruct.Mapper;

// Чтобы маппер стал Spring-бином
@Mapper(componentModel = "spring")
public interface TransferCommandMapper {
  Transfer toEntity(CreateTransferCommand createTransferCommand);
}
