package com.gmail.alexei28.shortcutkafkaproducer.task3.controller;

import com.gmail.alexei28.shortcutkafkaproducer.task3.command.CreateTransferCommand;
import com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces.RequestCommandMapper;
import com.gmail.alexei28.shortcutkafkaproducer.task3.request.CreateTransferRequest;
import com.gmail.alexei28.shortcutkafkaproducer.task3.service.TransferService;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/transfer")
public class TransferController {
  private final TransferService transferService;
  private final RequestCommandMapper requestCommandMapper;
  private static final Logger logger = LoggerFactory.getLogger(TransferController.class);

  public TransferController(
      TransferService transferService, RequestCommandMapper requestCommandMapper) {
    this.transferService = transferService;
    this.requestCommandMapper = requestCommandMapper;
  }

  /**
   * Sends a CreateTransferRequest object. E.g. POST http://localhost:8081/api/v1/transfer/create
   *
   * <p>Also need to add json body.
   *
   * <p>Spring Boot auto convert JSON to CreateTransferRequest
   */
  @PostMapping("/create")
  public ResponseEntity<?> createTransfer(
      @RequestHeader("Idempotency-Key") UUID idempotencyKey,
      @RequestBody CreateTransferRequest createTransferRequest) {
    logger.info("createTransfer, createTransferRequest = {}", createTransferRequest);
    CreateTransferCommand createTransferCommand =
        requestCommandMapper.toCommand(createTransferRequest, idempotencyKey);
    transferService.createTransfer(createTransferCommand);
    // ВАЖНО: 202 Accepted, НЕ 200
    return ResponseEntity.accepted().build();
  }
}
