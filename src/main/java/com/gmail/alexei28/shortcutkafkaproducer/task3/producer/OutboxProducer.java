package com.gmail.alexei28.shortcutkafkaproducer.task3.producer;

import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.OutboxDto;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repository.OutboxRepository;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/*
  Архитектура:

   Client
     ↓
   Transfer Service (DB transaction)
     ├── запись transfer
     ├── бухгалтерская проводка (ledger)
     └── запись в outbox
              ↓
        Outbox Publisher
              ↓
            Kafka
              ↓
   Receiving Bank Consumer
     └── idempotent processing
*/

@Service
public class OutboxProducer {
  @Value("${app.kafka.topics.task3}")
  private String topic;

  private final OutboxRepository outboxRepository;
  private final KafkaTemplate<String, OutboxDto> kafkaTemplate;
  private static final Logger logger = LoggerFactory.getLogger(OutboxProducer.class);

  public OutboxProducer(
      OutboxRepository outboxRepository, KafkaTemplate<String, OutboxDto> kafkaTemplate) {
    this.outboxRepository = outboxRepository;
    this.kafkaTemplate = kafkaTemplate;
  }

  @Scheduled(fixedDelay = 5000)
  @Transactional
  public void publish() {
    List<OutboxDto> outboxList = outboxRepository.lockBatchForPublish(50);
    logger.info("publish, outboxList({})", outboxList.size());
    for (OutboxDto outboxDto : outboxList) {
      try {
        kafkaTemplate.send(topic, outboxDto.getEventId().toString(), outboxDto);
        outboxDto.markPublished();
        logger.info("publish, successfully sent to Kafka outboxDto = {}", outboxDto);
      } catch (Exception e) {
        logger.error("publish, Error occurred while processing outboxDto: {}", e.getMessage(), e);
        outboxDto.markFailed();
      }
    }
  }
}
