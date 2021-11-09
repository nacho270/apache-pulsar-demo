package com.nacho.pulsar.demo.consumer.modes;

import com.nacho.pulsar.demo.consumer.Utils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.com.google.common.collect.Sets;

import java.util.Set;
import java.util.function.Function;

public class KeyedProducerStub {

  private final Producer<String> producer;
  private final Function<Integer, Integer> keyGenerator;

  public KeyedProducerStub(final PulsarClient pulsarClient, final String topic, final Function<Integer, Integer> keyGenerator) throws PulsarClientException {
    this.producer = Utils.producer(pulsarClient, topic);
    this.keyGenerator = keyGenerator;
  }

  public Set<String> produce(final int quantity) throws PulsarClientException {
    final Set<String> keys = Sets.newHashSet();
    for (int i = 0; i < quantity; i++) {
      final String message = "key-shared-msg-" + i;
      final String key = "key_" + keyGenerator.apply(i);
      keys.add(key);
      producer.newMessage().key(key).value(message).send();
    }
    return keys;
  }
}
