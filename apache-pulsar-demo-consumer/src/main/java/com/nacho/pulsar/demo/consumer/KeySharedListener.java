package com.nacho.pulsar.demo.consumer;

import java.util.concurrent.CompletableFuture;

import javax.annotation.PostConstruct;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeySharedListener {

  private final Consumer<String> pulsarKeySharedConsumer;

  @PostConstruct
  public void startListening() {
    CompletableFuture.supplyAsync(() -> {
      try {
        log.info("Listening on key-shared topic {} under subscription {}...", pulsarKeySharedConsumer.getTopic(), pulsarKeySharedConsumer.getSubscription());
        while (true) {
          final Message<String> msg = pulsarKeySharedConsumer.receive();

          log.info("Received message. Key: {}, value: {}", msg.getKey(), msg.getValue());

          pulsarKeySharedConsumer.acknowledge(msg);
        }
      } catch (final Exception e) {
        log.error("", e);
      }
      return null;
    });
  }

}
