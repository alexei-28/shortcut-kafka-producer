package com.gmail.alexei28.shortcutkafkaproducer.task3.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class OutboxPublisherService {
  private final OutboxTransactionalService outboxTransactionalService;
  private static final Logger logger = LoggerFactory.getLogger(OutboxPublisherService.class);

  public OutboxPublisherService(OutboxTransactionalService outboxTransactionalService) {
    this.outboxTransactionalService = outboxTransactionalService;
  }

  @Scheduled(fixedDelay = 500)
  public void publish() {
    try {
      outboxTransactionalService.publishOutboxEvent();
    } catch (Exception e) {
      logger.error("publish, Outbox publish failed", e);
    }
  }
}
