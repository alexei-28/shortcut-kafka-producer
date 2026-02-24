package com.gmail.alexei28.shortcutkafkaproducer.task3.controller;

import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.CreateOrderRequest;
import com.gmail.alexei28.shortcutkafkaproducer.task3.response.CreateOrderResponse;
import com.gmail.alexei28.shortcutkafkaproducer.task3.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/order")
public class OrderController {
  private final OrderService orderService;
  private static final Logger logger = LoggerFactory.getLogger(OrderController.class);

  public OrderController(OrderService orderService) {
    this.orderService = orderService;
  }

  @PostMapping("/create")
  public CreateOrderResponse createOrder(@RequestBody CreateOrderRequest createOrderRequest) {
    logger.info("crateOrder, createOrderRequest = {}", createOrderRequest);
    OrderEntity orderEntity = orderService.createOrder(createOrderRequest);
    return new CreateOrderResponse(orderEntity.getId(), orderEntity.getStatus());
  }
}
