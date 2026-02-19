package com.gmail.alexei28.shortcutkafkaproducer.interfaces;

import com.gmail.alexei28.shortcutkafkaproducer.dto.Task2Dto;
import com.gmail.alexei28.shortcutkafkaproducer.entity.Task2;
import org.mapstruct.Mapper;

// Чтобы маппер стал Spring-бином
@Mapper(componentModel = "spring")
public interface Task2Mapper {
  Task2Dto toDto(Task2 entity);

  Task2 toEntity(Task2Dto dto);
}
