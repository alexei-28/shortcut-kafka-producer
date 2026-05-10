package com.gmail.alexei28.shortcut.kafka.producer.task3.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.gmail.alexei28.shortcut.kafka.producer.task3.domain.OrderEntity;
import com.gmail.alexei28.shortcut.kafka.producer.task3.enums.OrderStatus;
import com.gmail.alexei28.shortcut.kafka.producer.task3.service.OrderService;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.util.StreamUtils;

@WebMvcTest(OrderController.class)
class OrderControllerTest {
  private static String jsonTemplate;
  private String createOrderRequestValidJson;

  @Autowired private MockMvc mockMvc;

  @MockitoBean private OrderService orderService;

  @BeforeAll
  static void beforeAll() throws IOException {
    jsonTemplate =
        StreamUtils.copyToString(
            new ClassPathResource("create_order_template.json").getInputStream(),
            StandardCharsets.UTF_8);
  }

  @BeforeEach
  void setUp() {
    // Update specific nodes in the JSON
    DocumentContext context =
        JsonPath.parse(jsonTemplate).set("$.externalId", UUID.randomUUID().toString());
    createOrderRequestValidJson = context.jsonString();
  }

  @Test
  void shouldReturnSuccessResponseWhenSendValidJson() throws Exception {
    // Arrange
    UUID orderId = UUID.randomUUID();
    OrderEntity orderEntity = new OrderEntity();
    orderEntity.setId(orderId);
    orderEntity.setStatus(OrderStatus.NEW);
    when(orderService.createOrder(any())).thenReturn(orderEntity);

    // Act and assert
    mockMvc
        .perform(
            post("/order/create")
                .contentType(MediaType.APPLICATION_JSON)
                .content(createOrderRequestValidJson))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.orderId").value(orderId.toString()))
        .andExpect(jsonPath("$.status").value(OrderStatus.NEW.name()));
  }

  @Test
  void shouldReturnDuplicateRequestError() throws Exception {
    // Arrange
    when(orderService.createOrder(any())).thenThrow(new IllegalStateException("Duplicate request"));

    // Act & Assert
    mockMvc
        .perform(
            post("/order/create")
                .contentType(MediaType.APPLICATION_JSON)
                .content(createOrderRequestValidJson))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value("Duplicate request"))
        .andExpect(jsonPath("$.timestamp").exists());
  }
}
