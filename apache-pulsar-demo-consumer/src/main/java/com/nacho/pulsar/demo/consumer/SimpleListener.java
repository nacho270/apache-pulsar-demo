package com.nacho.pulsar.demo.consumer;

import javax.annotation.PostConstruct;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SimpleListener {

  private final Consumer pulsarConsumer;

  public SimpleListener(final Consumer pulsarConsumer) {
    this.pulsarConsumer = pulsarConsumer;
  }

  @PostConstruct
  public void startListening() throws PulsarClientException {
    log.info("Listening on topic {} under subscription {}...", pulsarConsumer.getTopic(), pulsarConsumer.getSubscription());
    while (true) {
      final Message msg = pulsarConsumer.receive();

      log.info("Received message: " + msg.getData());

      pulsarConsumer.acknowledge(msg);
    }
  }

}
