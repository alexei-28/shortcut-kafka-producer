package com.gmail.alexei28.shortcutkafkaproducer.listeners;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/*
   Почему это сработает?
   Последовательность:
   Когда вы вызываете kafkaTemplate.send(), Spring сначала прогоняет данные через JsonSerializer (который добавляет __TypeId__),
       затем отправляет их в Kafka, и только после успешного ответа от брокера вызывает onSuccess в вашем ProducerListener.
   Доступ к данным:
   В onSuccess у вас есть доступ и к ProducerRecord (само сообщение), и к RecordMetadata (смещение, партиция).
*/
@Component
public class KafkaLoggingProducerListener implements ProducerListener<Object, Object> {
  private static final Logger logger = LoggerFactory.getLogger(KafkaLoggingProducerListener.class);

  @Override
  public void onSuccess(
      ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
    logger.info(
        "onSuccess, successfully sent, Topic: {}, Partition: {}, key :{}, Value: {}",
        producerRecord.topic(),
        recordMetadata.partition(),
        producerRecord.key(),
        producerRecord.value());

    // Здесь __TypeId__ уже будет присутствовать, так как сериализация завершена
    producerRecord
        .headers()
        .forEach(
            header -> {
              String value = new String(header.value());
              logger.info("onSuccess, Header: {} = {}", header.key(), value);
            });
  }

  @Override
  public void onError(
      ProducerRecord<Object, Object> producerRecord,
      RecordMetadata recordMetadata,
      Exception exception) {
    logger.error("onError, Error sending message to topic: {}", producerRecord.topic(), exception);
  }
}
