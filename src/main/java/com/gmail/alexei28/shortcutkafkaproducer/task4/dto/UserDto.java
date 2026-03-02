package com.gmail.alexei28.shortcutkafkaproducer.task4.dto;

import java.util.UUID;

public record UserDto(
    UUID eventId,
    UUID userId,
    String firstName,
    String lastName,
    String email,
    String inn,
    String address) {
  @Override
  public String toString() {
    return "UserDto{"
        + "eventId="
        + eventId
        + ", userId="
        + userId
        + ", firstName='"
        + firstName
        + '\''
        + ", lastName='"
        + lastName
        + '\''
        + ", email='"
        + email
        + '\''
        + ", inn='"
        + inn
        + '\''
        + ", address='"
        + address
        + '\''
        + '}';
  }
}
