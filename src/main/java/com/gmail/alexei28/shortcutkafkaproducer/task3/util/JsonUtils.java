package com.gmail.alexei28.shortcutkafkaproducer.task3.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonUtils {
  private static final ObjectMapper mapper =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT); // Включаем отступы

  public static String toPrettyJson(String jsonString) {
    try {
      Object jsonObject = mapper.readValue(jsonString, Object.class);
      return mapper.writeValueAsString(jsonObject);
    } catch (Exception e) {
      return jsonString; // Если это не JSON, возвращаем как есть
    }
  }
}
