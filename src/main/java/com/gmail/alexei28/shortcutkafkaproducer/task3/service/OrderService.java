package com.gmail.alexei28.shortcutkafkaproducer.task3.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces.OrderEntityOutboxEventMapping;
import com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces.RequestOrderEntityMapping;
import com.gmail.alexei28.shortcutkafkaproducer.task3.outbox.OutboxEvent;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repository.OrderRepository;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repository.OutboxRepository;
import com.gmail.alexei28.shortcutkafkaproducer.task3.request.CreateOrderRequest;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/*
    OrderService — атомарная запись
    ОДНА ТРАНЗАКЦИЯ:
     -записали order
     -записали outbox
     -commit
*/
@Service
public class OrderService {
  @Value("${app.kafka.topics.task3}")
  private String topic;

  private final RequestOrderEntityMapping requestOrderEntityMapping;
  private final OrderEntityOutboxEventMapping orderEntityOutboxEventMapping;
  private final OrderRepository orderRepository;
  private final OutboxRepository outboxRepository;
  private final ObjectMapper objectMapper;
  private static final Logger logger = LoggerFactory.getLogger(OrderService.class);

  public OrderService(
      RequestOrderEntityMapping requestOrderEntityMapping,
      OrderEntityOutboxEventMapping orderEntityOutboxEventMapping,
      OrderRepository orderRepository,
      OutboxRepository outboxRepository,
      ObjectMapper objectMapper) {
    this.requestOrderEntityMapping = requestOrderEntityMapping;
    this.orderEntityOutboxEventMapping = orderEntityOutboxEventMapping;
    this.orderRepository = orderRepository;
    this.outboxRepository = outboxRepository;
    this.objectMapper = objectMapper;
  }

  @Transactional
  public OrderEntity createOrder(CreateOrderRequest createOrderRequest) {
    logger.info("createOrder, createOrderRequest = {}", createOrderRequest);
    // 1. проверка бизнес-идемпотентности
    orderRepository
        .findByExternalId(createOrderRequest.externalId())
        .ifPresent(
            o -> {
              logger.warn(
                  "createOrder, Order already exists, externalId = {}",
                  createOrderRequest.externalId());
              throw new IllegalStateException("Order already exists");
            });

    // 2. создаем заказ
    OrderEntity orderEntity = requestOrderEntityMapping.toEntity(createOrderRequest);
    orderRepository.save(orderEntity);
    logger.info("createOrder, successfully saved to repo orderEntity = {}", orderEntity);

    // 3. создаем outbox event
    try {
      OutboxEvent outboxEvent = orderEntityOutboxEventMapping.toOutboxEvent(orderEntity);
      outboxRepository.save(outboxEvent);
      logger.info("createOrder, successfully saved to repo outboxEvent = {}", outboxEvent);
    } catch (Exception e) {
      logger.error("createOrder, Error {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
    logger.info("createOrder, successfully return orderEntity = {}", orderEntity);
    return orderEntity;
  }
}
