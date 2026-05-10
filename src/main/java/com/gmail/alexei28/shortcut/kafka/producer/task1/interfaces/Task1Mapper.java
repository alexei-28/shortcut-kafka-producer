package com.gmail.alexei28.shortcut.kafka.producer.task1.interfaces;

import com.gmail.alexei28.shortcut.kafka.producer.task1.dto.Task1Dto;
import com.gmail.alexei28.shortcut.kafka.producer.task1.entity.Task1;
import org.mapstruct.Mapper;

// Чтобы маппер стал Spring-бином
@Mapper(componentModel = "spring")
public interface Task1Mapper {
  // Entity -> DTO (для продюсера)
  // Если имена полей совпадают (number, content, receivedAt),
  // MapStruct свяжет их автоматически.
  Task1Dto toDto(Task1 entity);

  // DTO -> Entity (для консьюмера)
  // Если бы имена отличались, мы бы писали так:
  // @Mapping(source = "content", target = "textContent")
  Task1 toEntity(Task1Dto dto);
}
