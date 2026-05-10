package com.gmail.alexei28.shortcut.kafka.producer.task5.request;

import java.util.UUID;

public record UserCreateRequest(
    UUID userId, String firstName, String lastName, String email, String inn, String address) {
  @Override
  public String toString() {
    return "\nUserCreateRequest{"
        + "\nuserId="
        + userId
        + ",\n firstName='"
        + firstName
        + '\''
        + ",\n lastName='"
        + lastName
        + '\''
        + ",\n email='"
        + email
        + '\''
        + ",\n inn='"
        + inn
        + '\''
        + ",\n address='"
        + address
        + '\''
        + '}';
  }
}
