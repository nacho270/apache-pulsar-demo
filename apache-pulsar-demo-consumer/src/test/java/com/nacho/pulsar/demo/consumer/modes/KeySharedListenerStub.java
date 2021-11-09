package com.nacho.pulsar.demo.consumer.modes;

import com.nacho.pulsar.demo.consumer.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class KeySharedListenerStub {

  private final Consumer<String> pulsarKeySharedConsumer;
  private final Map<String, List<String>> consumed = Maps.newHashMap();

  public KeySharedListenerStub(final PulsarClient pulsarClient, final String topic, final String subscription) throws PulsarClientException {
    pulsarKeySharedConsumer = Utils.consumer(pulsarClient, topic, subscription, SubscriptionType.Key_Shared);
  }

  public void startListening() {
    new Thread(() -> {
      try {
        while (pulsarKeySharedConsumer.isConnected()) {
          final Message<String> msg = pulsarKeySharedConsumer.receive();
          log.info("Received message. Key: {}, value: {}", msg.getKey(), msg.getValue());

          consumed.computeIfAbsent(msg.getKey(), k -> new ArrayList<>()).add(msg.getValue());

          pulsarKeySharedConsumer.acknowledge(msg);
        }
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }).start();
  }

  public int consumedCount() {
    return consumed.values().stream().map(List::size).reduce(0, Integer::sum);
  }

  public Set<String> keys() {
    return consumed.keySet();
  }

  public List<String> forKey(final String key) {
    return consumed.get(key);
  }
}
