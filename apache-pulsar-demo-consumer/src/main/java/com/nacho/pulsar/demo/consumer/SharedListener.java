package com.nacho.pulsar.demo.consumer;

import javax.annotation.PostConstruct;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class SharedListener {

  private final Consumer<String> pulsarSharedConsumer;

  @PostConstruct
  public void startListening() {
    new Thread(() -> {
      try {
        log.info("Listening on  shared topic {} under subscription {}...", pulsarSharedConsumer.getTopic(), pulsarSharedConsumer.getSubscription());
        while (true) {
          final Message<String> msg = pulsarSharedConsumer.receive();

          log.info("Received message: " + msg.getValue());

          pulsarSharedConsumer.acknowledge(msg);
        }
      } catch (final Exception e) {
        log.error("", e);
      }
    }).start();
  }

}
