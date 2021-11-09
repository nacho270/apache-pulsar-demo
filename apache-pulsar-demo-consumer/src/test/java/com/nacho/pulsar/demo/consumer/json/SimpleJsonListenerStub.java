package com.nacho.pulsar.demo.consumer.json;

import com.nacho.pulsar.demo.consumer.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SimpleJsonListenerStub {

  private final Consumer<UserJson> pulsarSharedConsumer;
  private final List<UserJson> consumed = new ArrayList<>();

  public SimpleJsonListenerStub(final PulsarClient pulsarClient, final String topic, final String subscription) throws PulsarClientException {
    pulsarSharedConsumer = Utils.jsonConsumer(pulsarClient, topic, subscription, SubscriptionType.Shared, UserJson.class);
  }

  public void startListening() {
    new Thread(() -> {
      try {
        while (pulsarSharedConsumer.isConnected()) {
          final Message<UserJson> msg = pulsarSharedConsumer.receive(100, TimeUnit.MILLISECONDS);
          if (msg != null) {
            consumed.add(msg.getValue());
            pulsarSharedConsumer.acknowledge(msg);
          }
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
