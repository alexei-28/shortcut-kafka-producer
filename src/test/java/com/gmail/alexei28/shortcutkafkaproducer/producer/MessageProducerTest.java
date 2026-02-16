package com.gmail.alexei28.shortcutkafkaproducer.producer;

import static org.mockito.Mockito.*;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
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
  private static String jsonTemplate;
  private String producedValidJson;
  private final long randomNumber = new Random().nextLong(10000);

  @BeforeAll
  static void beforeAll() throws IOException {
    jsonTemplate =
        StreamUtils.copyToString(
            new ClassPathResource("message_template.json").getInputStream(),
            StandardCharsets.UTF_8);
  }

  @BeforeEach
  void setUp() {
    // Update specific nodes in the JSON
    DocumentContext context =
        JsonPath.parse(jsonTemplate)
            .set("$.number", randomNumber)
            .set("$.content", "Message_TEST_PRODUCED_" + randomNumber);
    producedValidJson = context.jsonString();
  }

  @Test
  @DisplayName("Producer should send raw message")
  void shouldSendValueMessage() {
    // Act
    messageProducer.sendValue(TOPIC, producedValidJson);
    // Assert
    verify(kafkaTemplateMock).send(TOPIC, producedValidJson);
  }
}
