package com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces;

import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.Transfer;
import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.OutboxDto;
import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.OutboxStatus;
import java.util.UUID;
import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;

// Чтобы маппер стал Spring-бином
@Mapper(
    componentModel = "spring",
    imports = {UUID.class})
public interface OutboxDtoMapper {
  // Если имена остальных полей совпадают (operationId, amount и т.д.), то MapStruct свяжет их
  // автоматически.
  @Mapping(target = "eventId", expression = "java(UUID.randomUUID())")
  @Mapping(target = "status", ignore = true)
  OutboxDto toDto(Transfer transfer);

  @AfterMapping
  default void initStatus(@MappingTarget OutboxDto dto) {
    dto.setStatus(OutboxStatus.NEW);
  }
}
