package com.nacho.pulsar.demo.producer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PulsarProducer {

  private final Producer<String> pulsarKeySharedProducer;
  private final Producer<String> pulsarSharedProducer;

  public void produceKeyShared() throws PulsarClientException {
    for (int i = 0; i < 10; i++) {
      final String message = "key-shared-msg-" + i;
      final String key = "key_" + i % 2;
      log.info("Sending msg with key: {}, value: {}", key, message);
      pulsarKeySharedProducer.newMessage().key(key).value(message).send();
    }
  }

  public void produceShared() throws PulsarClientException {
    for (int i = 0; i < 10; i++) {
      final String message = "shared-message-" + i;
      log.info("Sending msg: {}", message);
      pulsarSharedProducer.newMessage().value(message).send();
    }
  }
}
