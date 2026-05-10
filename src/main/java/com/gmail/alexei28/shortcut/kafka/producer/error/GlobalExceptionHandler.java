package com.gmail.alexei28.shortcut.kafka.producer.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/*
   Как это работает:
   1. @RestControllerAdvice: Говорит Spring, что этот класс перехватывает исключения, выбрасываемые во всех контроллерах приложения.
   2. @ExceptionHandler(IllegalStateException.class): Указывает, что данный метод должен сработать именно тогда,
      когда в коде(например, в OrderService) летит IllegalStateException.
   3. Автоматическая сериализация: Поскольку это RestControllerAdvice, Spring автоматически преобразует ваш объект ErrorResponse в JSON,
      используя Jackson.
*/
@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(IllegalStateException.class)
  public ResponseEntity<ErrorResponse> handleIllegalStateException(IllegalStateException ex) {
    // Создаем объект ErrorResponse
    ErrorResponse error = new ErrorResponse(ex.getMessage());
    // Возвращаем его с подходящим HTTP статусом (например, 400 Bad Request или 409 Conflict)
    return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
  }

  // Обработка всех остальных непредвиденных исключений
  @ExceptionHandler(Exception.class)
  public ResponseEntity<ErrorResponse> handleGeneralException(Exception ex) {
    ErrorResponse error = new ErrorResponse("An unexpected error occurred: " + ex.getMessage());
    return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
