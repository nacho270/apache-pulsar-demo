package com.nacho.pulsar.demo.consumer.json;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserJson {

  private UUID id;
  private String name;
  private String email;
}
