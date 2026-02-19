package com.gmail.alexei28.shortcutkafkaproducer.task2.producer;

import com.gmail.alexei28.shortcutkafkaproducer.task2.dto.CashbackDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/*
   Steps:
   1.Browser (JSON)
   2.Jackson (превращает в Cashback)
   3.Controller (получает Cashback)
   4.Producer (вызывает MapStruct)
   5.MapStruct (превращает Cashback в CashbackDto)
   6.Kafka (отправляет CashbackDto как JSON)
*/
@Service
public class CashbackProducer {
  @Value("${app.kafka.topics.task2}")
  private String topic;

  private static final Logger logger = LoggerFactory.getLogger(CashbackProducer.class);

  private final KafkaTemplate<String, CashbackDto> kafkaTemplate;

  public CashbackProducer(KafkaTemplate<String, CashbackDto> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendCashback(CashbackDto cashbackDto) {
    sendCashback(this.topic, cashbackDto);
  }

  public void sendCashback(String topic, CashbackDto cashbackDto) {
    kafkaTemplate
        .send(topic, cashbackDto.getEventId(), cashbackDto)
        .whenComplete(
            (result, ex) -> {
              if (ex != null) {
                logger.error("sendCashback, send failed: {}", ex.getMessage());
              } else {
                logger.info(
                    "sendCashback, successfully sent to topic: {}",
                    result.getRecordMetadata().topic());
              }
            });
  }
}
