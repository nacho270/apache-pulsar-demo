package com.nacho.pulsar.demo.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.com.google.common.collect.Maps;
import org.apache.pulsar.shade.com.google.common.collect.Sets;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
class Utils {

  static class ExclusiveListenerStub {

    private final Consumer<String> pulsarSharedConsumer;
    private final List<String> consumed = Collections.synchronizedList(new ArrayList<>());

    ExclusiveListenerStub(final PulsarClient pulsarClient, final String topic, final String subscription) throws PulsarClientException, PulsarAdminException {
      pulsarSharedConsumer = consumer(pulsarClient, topic, subscription, SubscriptionType.Exclusive);
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

  static class SharedListenerStub {

    private final Consumer<String> pulsarSharedConsumer;
    private final List<String> consumed = Collections.synchronizedList(new ArrayList<>());

    SharedListenerStub(final PulsarClient pulsarClient, final String topic, final String subscription) throws PulsarClientException, PulsarAdminException {
      pulsarSharedConsumer = consumer(pulsarClient, topic, subscription, SubscriptionType.Shared);
    }

    public void startListening() {
      new Thread(() -> {
        try {
          while (pulsarSharedConsumer.isConnected()) {
            final Message<String> msg = pulsarSharedConsumer.receive(100, TimeUnit.MILLISECONDS);
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

    public void close() throws PulsarClientException {
      pulsarSharedConsumer.close();
    }
  }

  static class KeySharedListenerStub {

    private final Consumer<String> pulsarKeySharedConsumer;
    private final Map<String, List<String>> consumed = Maps.newConcurrentMap();

    KeySharedListenerStub(final PulsarClient pulsarClient, final String topic, final String subscription) throws PulsarClientException, PulsarAdminException {
      pulsarKeySharedConsumer = consumer(pulsarClient, topic, subscription, SubscriptionType.Key_Shared);
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

    public void close() throws PulsarClientException {
      pulsarKeySharedConsumer.close();
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

  static class SimpleProducerStub {

    private final Producer<String> producer;

    public SimpleProducerStub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
      this.producer = producer(pulsarClient, topic);
    }

    public void produce(final int quantity) throws Exception {
      for (int i = 0; i < quantity; i++) {
        final String message = "shared-message-" + i;
        producer.newMessage().value(message).send();
        sleep(100);
      }
    }

    public void produceAsync(final int quantity) throws Exception {
      new Thread(() -> {
        try {
          for (int i = 0; i < quantity; i++) {
            final String message = "shared-message-" + i;
            producer.newMessage().value(message).send();
            sleep(100);
          }
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }).start();
    }
  }

  static class KeyedProducerStub {

    private final Producer<String> producer;
    private final Function<Integer, Integer> keyGenerator;

    public KeyedProducerStub(final PulsarClient pulsarClient, final String topic, final Function<Integer, Integer> keyGenerator) throws PulsarClientException {
      this.producer = producer(pulsarClient, topic);
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

    public void produceAsync(final int quantity) throws PulsarClientException {
      new Thread(() -> {
        try {
          for (int i = 0; i < quantity; i++) {
            final String message = "key-shared-msg-" + i;
            final String key = "key_" + keyGenerator.apply(i);
            producer.newMessage().key(key).value(message).send();
          }
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }).start();
    }
  }

  private static Consumer<String> consumer(final PulsarClient pulsarClient, final String topic, final String subscription, final SubscriptionType subscriptionType)
      throws PulsarClientException, PulsarAdminException {
    return pulsarClient //
        .newConsumer(Schema.STRING) //
        .topic(topic) //
        .subscriptionName(subscription) //
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) //
        .subscriptionType(subscriptionType) //
        .receiverQueueSize(10) //
        .subscribe();
  }

  private static Producer<String> producer(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    return pulsarClient //
        .newProducer(Schema.STRING) //
        .topic(topic) //
        .create();
  }

  static void sleep(final long millis) throws Exception {
    TimeUnit.MILLISECONDS.sleep(millis);
  }
}
