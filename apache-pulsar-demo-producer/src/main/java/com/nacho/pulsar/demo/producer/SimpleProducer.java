package com.nacho.pulsar.demo.producer;

import javax.annotation.PostConstruct;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Service;

@Service
public class SimpleProducer {

  private final Producer pulsarProducer;

  public SimpleProducer(final Producer pulsarProducer) {
    this.pulsarProducer = pulsarProducer;
  }

  @PostConstruct
  public void sendSample() throws PulsarClientException {
    for (int i = 0; i < 10; i++) {
      pulsarProducer.send("my-message".getBytes());
    }
  }
}
