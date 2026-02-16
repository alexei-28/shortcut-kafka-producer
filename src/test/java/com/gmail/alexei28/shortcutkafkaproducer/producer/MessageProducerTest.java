package com.gmail.alexei28.shortcutkafkaproducer.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.gmail.alexei28.shortcutkafkaproducer.dto.Message;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.StreamUtils;

@ExtendWith(MockitoExtension.class)
class MessageProducerTest {
  private static final String TOPIC = "test_message_topic";
  @Mock private KafkaTemplate<String, Object> kafkaTemplateMock;

  // @InjectMocks НЕ создает mock. Она создает реальный экземпляр вашего класса, чтобы вы могли его
  // протестировать.
  // @InjectMocks — это "живой" объект (ваш сервис), в который вставили эти манекены в качестве
  // запчастей.
  @InjectMocks private MessageProducer messageProducer;
  private Message producedMessage;
  private String producedMessageJson;

  @BeforeEach
  void setUp() throws IOException {
    producedMessage =
        new Message(
            System.currentTimeMillis(),
            "MessageLog_Test_".concat(String.valueOf(System.currentTimeMillis())),
            LocalDateTime.now());

    producedMessageJson =
        StreamUtils.copyToString(
            new ClassPathResource("message.json").getInputStream(), StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("Producer should send raw message")
  void shouldSendRawMessage() {
    // Act
    messageProducer.sendRaw(TOPIC, producedMessageJson);
    // Assert
    verify(kafkaTemplateMock).send(TOPIC, producedMessageJson);
  }

  @Test
  @DisplayName("Producer should send success object Message")
  void testSendMessage_Success() {
    // Arrange
    CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
    when(kafkaTemplateMock.send(anyString(), any())).thenReturn(future);
    // Act
    messageProducer.sendMessage(TOPIC, producedMessage);
    // Assert
    // Завершаем future успешно
    future.complete(mock(SendResult.class));
    verify(kafkaTemplateMock).send(TOPIC, producedMessage);
  }
}
