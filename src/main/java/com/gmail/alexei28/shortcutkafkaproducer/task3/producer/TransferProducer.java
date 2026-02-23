package com.gmail.alexei28.shortcutkafkaproducer.task3.producer;

import com.gmail.alexei28.shortcutkafkaproducer.task3.domain.Transfer;
import com.gmail.alexei28.shortcutkafkaproducer.task3.dto.TransferDto;
import com.gmail.alexei28.shortcutkafkaproducer.task3.interfaces.TransferMapper;
import com.gmail.alexei28.shortcutkafkaproducer.task3.repository.TransferRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TransferProducer {
  @Value("${app.kafka.topics.task3}")
  private String topic;

  private final TransferMapper mapper;
  private final TransferRepository repository;
  private final KafkaTemplate<String, TransferDto> kafkaTemplate;

  private static final Logger logger = LoggerFactory.getLogger(TransferProducer.class);

  public TransferProducer(
      KafkaTemplate<String, TransferDto> kafkaTemplate,
      TransferMapper mapper,
      TransferRepository repository) {
    this.kafkaTemplate = kafkaTemplate;
    this.mapper = mapper;
    this.repository = repository;
  }

  public void sendTransfer(TransferDto transferDto) {
    sendTransfer(this.topic, transferDto);
  }

  @Transactional
  public void sendTransfer(String topic, TransferDto transferDto) {
    Transfer transfer = mapper.toEntity(transferDto);
    repository.save(transfer);
    kafkaTemplate.send(topic, transferDto);
  }
}
