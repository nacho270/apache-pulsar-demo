package com.nacho.pulsar.demo.consumer.avro.object;

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
public class SimpleAvroObjectListenerV2Stub {

  private final Consumer<Object> pulsarSharedConsumer;

  private final List<Object> consumed = new ArrayList<>();

  public SimpleAvroObjectListenerV2Stub(final PulsarClient pulsarClient, final String topic, final String subscription) throws PulsarClientException {
    pulsarSharedConsumer = Utils.objectConsumerV2(pulsarClient, topic, subscription, SubscriptionType.Shared);
  }

  public void startListening() {
    new Thread(() -> {
      try {
        while (pulsarSharedConsumer.isConnected()) {
          final Message<Object> msg = pulsarSharedConsumer.receive(100, TimeUnit.MILLISECONDS);
          if (msg != null) {
            log.info("Received message: " + msg.getValue());
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
