package com.gmail.alexei28.shortcutkafkaproducer.task3.dto;

public enum OutboxStatus {
  NEW, // только записано
  PUBLISHED, // отправлено в Kafka
  FAILED // ошибка, будет retry
}
