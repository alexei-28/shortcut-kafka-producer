package com.gmail.alexei28.shortcutkafkaproducer.task3.controller;

import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.idempotency.IdempotencyKeyEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repository.IdempotencyRepository;
import com.gmail.alexei28.shortcutkafkaproducer.task3.request.CreateOrderRequest;
import com.gmail.alexei28.shortcutkafkaproducer.task3.service.OrderService;
import jakarta.transaction.Transactional;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/*
   Это защита от:
    - Double click (e.g. on frontend client double click on button
    - Retry mobile

    Example of request: /shortcut-kafka-producer/restclient.rc
*/
@RestController
@RequestMapping("/order")
public class OrderController {
  private final IdempotencyRepository idempotencyRepository;
  private final OrderService orderService;
  private static final Logger logger = LoggerFactory.getLogger(OrderController.class);

  public OrderController(IdempotencyRepository idempotencyRepository, OrderService orderService) {
    this.idempotencyRepository = idempotencyRepository;
    this.orderService = orderService;
  }

  @PostMapping("/create")
  @Transactional
  public ResponseEntity<?> createOrder(
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestBody CreateOrderRequest createOrderRequest) {
    logger.info(
        "createOrder, idempotencyKey = {}, createOrderRequest = {}",
        idempotencyKey,
        createOrderRequest);
    // 1. проверяем ключ
    Optional<IdempotencyKeyEntity> findIdempotencyKeyEntity =
        idempotencyRepository.findByIdempotencyKey(idempotencyKey);
    if (findIdempotencyKeyEntity.isPresent()) {
      IdempotencyKeyEntity idempotencyKeyEntity = findIdempotencyKeyEntity.get();
      logger.warn(
          "createOrder, IdempotencyKeyEntity has been already created: {}",
          findIdempotencyKeyEntity);
      return ResponseEntity.status(idempotencyKeyEntity.getResponseCode())
          .body(idempotencyKeyEntity.getResponseBody());
    }

    // 2. выполняем операцию
    OrderEntity orderEntity = orderService.createOrder(createOrderRequest);
    logger.info("createOrder, successfully created orderEntity = {}", orderEntity);
    String responseBody = "OrderEntity created, id = " + orderEntity.getId();

    // 3. сохраняем ответ
    IdempotencyKeyEntity idempotencyKeyEntity = new IdempotencyKeyEntity();
    idempotencyKeyEntity.setIdempotencyKey(idempotencyKey);
    idempotencyKeyEntity.setResponseCode(HttpStatus.OK.value());
    idempotencyKeyEntity.setResponseBody(responseBody);
    idempotencyRepository.save(idempotencyKeyEntity);
    logger.info(
        "createOrder, successfully saved to repo, idempotencyKeyEntity = {}", idempotencyKeyEntity);
    return ResponseEntity.ok(responseBody);
  }
}
