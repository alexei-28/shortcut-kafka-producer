package com.gmail.alexei28.shortcutkafkaproducer.task3.response;

import com.gmail.alexei28.shortcutkafkaproducer.task3.enums.OrderStatus;
import java.util.UUID;

public record CreateOrderResponse(UUID orderId, OrderStatus status) {}
