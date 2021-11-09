package com.nacho.pulsar.demo.consumer.modes;

import com.nacho.pulsar.demo.consumer.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class ExclusiveListenerStub {

  private final Consumer<String> pulsarSharedConsumer;
  private final List<String> consumed = Collections.synchronizedList(new ArrayList<>());

  public ExclusiveListenerStub(final PulsarClient pulsarClient, final String topic, final String subscription) throws PulsarClientException {
    pulsarSharedConsumer = Utils.consumer(pulsarClient, topic, subscription, SubscriptionType.Exclusive);
  }

  public void startListening() {
    new Thread(() -> {
      try {
        while (pulsarSharedConsumer.isConnected()) {
          final Message<String> msg = pulsarSharedConsumer.receive();
          log.info("Received message: " + msg.getValue());
          consumed.add(msg.getValue());
          pulsarSharedConsumer.acknowledge(msg);
        }
        log.info("Consumer closed");
      } catch (final Exception e) {
        e.printStackTrace();
      }

    }).start();
  }

  public int consumedCount() {
    return consumed.size();
  }
}
