package com.nacho.pulsar.demo.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PulsarConsumerMain {

  public static void main(final String[] args) {
    SpringApplication.run(PulsarConsumerMain.class, args);
  }
}
