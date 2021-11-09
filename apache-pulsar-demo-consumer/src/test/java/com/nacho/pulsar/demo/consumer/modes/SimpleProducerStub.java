package com.nacho.pulsar.demo.consumer.modes;

import com.nacho.pulsar.demo.consumer.Utils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class SimpleProducerStub {

  private final Producer<String> producer;

  public SimpleProducerStub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    this.producer = Utils.producer(pulsarClient, topic);
  }

  public void produce(final int quantity) throws Exception {
    for (int i = 0; i < quantity; i++) {
      final String message = "shared-message-" + i;
      producer.newMessage().value(message).send();
      Utils.sleep(100);
    }
  }

  public void produceAsync(final int quantity) {
    new Thread(() -> {
      try {
        for (int i = 0; i < quantity; i++) {
          final String message = "shared-message-" + i;
          producer.newMessage().value(message).send();
          Utils.sleep(100);
        }
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }).start();
  }
}
