package com.gmail.alexei28.shortcutkafkaproducer.task3.service;

import com.gmail.alexei28.shortcutkafkaproducer.task3.command.CreateTransferCommand;
import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.Transfer;
import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.OutboxDto;
import com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces.OutboxDtoMapper;
import com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces.TransferCommandMapper;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repository.OutboxRepository;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repository.TransferRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransferService {
  private final TransferCommandMapper transferCommandMapper;
  private final OutboxDtoMapper outboxDtoMapper;
  private final TransferRepository transferRepository;
  private final OutboxRepository outboxRepository;
  private static final Logger logger = LoggerFactory.getLogger(TransferService.class);

  public TransferService(
      TransferCommandMapper transferCommandMapper,
      OutboxDtoMapper outboxDtoMapper,
      TransferRepository transferRepository,
      OutboxRepository outboxRepository) {
    this.transferCommandMapper = transferCommandMapper;
    this.outboxDtoMapper = outboxDtoMapper;
    this.transferRepository = transferRepository;
    this.outboxRepository = outboxRepository;
  }

  @Transactional
  public void createTransfer(CreateTransferCommand createTransferCommand) {
    logger.info("createTransfer, createTransferCommand = {}", createTransferCommand);
    // идемпотентность запроса
    if (transferRepository.existsByOperationId(createTransferCommand.operationId())) {
      logger.warn(
          "createTransfer, already exist operationId = {}", createTransferCommand.operationId());
      return; // safe retry клиента
    }

    // создаем перевод
    Transfer transfer = transferCommandMapper.toEntity(createTransferCommand);
    transferRepository.save(transfer);
    logger.info("createTransfer, successfully saved to repo transfer = {}", transfer);

    // outbox event
    OutboxDto outboxDto = outboxDtoMapper.toDto(transfer);
    logger.info("createTransfer, mapped outboxDto: {}", outboxDto);
    outboxRepository.save(outboxDto);
    logger.info("createTransfer, successfully saved to repo outboxDto = {}", outboxDto);
  }
}
