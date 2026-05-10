package com.gmail.alexei28.shortcut.kafka.producer.task3.service;

import com.gmail.alexei28.shortcut.kafka.producer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcut.kafka.producer.task3.dto.CreateOrderRequest;
import com.gmail.alexei28.shortcut.kafka.producer.task3.event.CreateOrderEvent;
import com.gmail.alexei28.shortcut.kafka.producer.task3.event.OutboxEvent;
import com.gmail.alexei28.shortcut.kafka.producer.task3.mapper.CreateOrderRequestOrderEntityMapper;
import com.gmail.alexei28.shortcut.kafka.producer.task3.mapper.OrderEntityCreateOrderEventMapper;
import com.gmail.alexei28.shortcut.kafka.producer.task3.mapper.OrderEntityOutboxEventMapper;
import com.gmail.alexei28.shortcut.kafka.producer.task3.repo.OrderRepository;
import com.gmail.alexei28.shortcut.kafka.producer.task3.repo.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/*
  Схема:
   1. Сохраняем OrderEntity
   2. Сохраняем OutboxEvent
   3. Всё в одной транзакции
   Если падает что угодно → откатится и заказ, и событие.
*/
@Service
public class OrderService {
  private final OrderRepository orderRepository;
  private final OutboxEventRepository outboxEventRepository;
  private final CreateOrderRequestOrderEntityMapper createOrderRequestOrderEntityMapper;
  private final OrderEntityCreateOrderEventMapper orderEntityCreateOrderEventMapper;
  private final OrderEntityOutboxEventMapper orderEntityOutboxEventMapper;
  private static final Logger logger = LoggerFactory.getLogger(OrderService.class);

  public OrderService(
      OrderRepository orderRepository,
      OutboxEventRepository outboxEventRepository,
      CreateOrderRequestOrderEntityMapper createOrderRequestOrderEntityMapper,
      OrderEntityCreateOrderEventMapper orderEntityCreateOrderEventMapper,
      OrderEntityOutboxEventMapper orderEntityOutboxEventMapper) {
    this.orderRepository = orderRepository;
    this.outboxEventRepository = outboxEventRepository;
    this.createOrderRequestOrderEntityMapper = createOrderRequestOrderEntityMapper;
    this.orderEntityCreateOrderEventMapper = orderEntityCreateOrderEventMapper;
    this.orderEntityOutboxEventMapper = orderEntityOutboxEventMapper;
  }

  // Атомарная запись OrderEntity + OutboxEvent
  @Transactional
  public OrderEntity createOrder(CreateOrderRequest createOrderRequest) {
    logger.info(
        "createOrder, createOrderRequest = {}, isActualTransactionActive = {}",
        createOrderRequest,
        TransactionSynchronizationManager.isActualTransactionActive());
    orderRepository
        .findByExternalId(createOrderRequest.externalId())
        .ifPresent(
            o -> {
              throw new IllegalStateException("Duplicate request");
            });

    OrderEntity orderEntity = createOrderRequestOrderEntityMapper.toEntity(createOrderRequest);
    orderRepository.save(orderEntity);
    logger.info("createOrder, successfully saved to repo, orderEntity = {}", orderEntity);
    try {
      // createOrderEvent в виде json будет записан в поле OutboxEvent.payload
      CreateOrderEvent createOrderEvent =
          orderEntityCreateOrderEventMapper.toOrderEvent(orderEntity);
      OutboxEvent outboxEvent =
          orderEntityOutboxEventMapper.toOutboxEvent(orderEntity, createOrderEvent);
      outboxEventRepository.save(outboxEvent);
      logger.info("createOrder, successfully saved to repo, outboxEvent = {}", outboxEvent);
      return orderEntity;
    } catch (Exception e) {
      logger.error("createOrder, error", e);
      throw new RuntimeException(e);
    }
  }
}
