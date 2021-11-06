package com.nacho.pulsar.demo.producer;

import javax.annotation.PostConstruct;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SimpleProducer {

  private final Producer<String> pulsarProducer;

  public SimpleProducer(final Producer<String> pulsarProducer) {
    this.pulsarProducer = pulsarProducer;
  }

  @PostConstruct
  public void sendSample() throws PulsarClientException {
    for (int i = 0; i < 10; i++) {
      final String message = "my-message-" + i;
      log.info("Sending msg: {}", message);
      pulsarProducer.send(message);
    }
  }
}
