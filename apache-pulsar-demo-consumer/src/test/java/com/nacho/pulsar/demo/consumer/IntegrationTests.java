package com.nacho.pulsar.demo.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class IntegrationTests {

  @Container
  private static final PulsarContainer pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:latest"));

  private static PulsarClient pulsarClient;

  @BeforeAll
  public static void init() throws PulsarClientException {
    pulsarClient = PulsarClient.builder() //
        .serviceUrl(pulsarContainer.getPulsarBrokerUrl()) //
        .build();
  }

  @Test
  void produceToSingleSharedConsumer() throws Exception {
    // given
    final String topic = "produceToSingleSharedConsumer";
    final int quantity = 10;
    final Utils.SimpleProducerStub producer = new Utils.SimpleProducerStub(pulsarClient, topic);
    final Utils.SharedListenerStub listener = new Utils.SharedListenerStub(pulsarClient, topic, "subscription-produceToSingleSharedConsumer");
    listener.startListening();

    // when
    producer.produce(quantity);

    // then
    await() //
        .pollDelay(Duration.ofSeconds(1L))//
        .atMost(Duration.ofSeconds(10)) //
        .pollInterval(Duration.ofMillis(100)) //
        .untilAsserted(() -> assertThat(listener.consumedCount()).isEqualTo(quantity));
  }

  @Test
  void produceToMultipleSharedConsumers() throws Exception {
    // given
    final String topic = "produceToMultipleSharedConsumers";
    final int quantity = 10;
    final Utils.SimpleProducerStub producer = new Utils.SimpleProducerStub(pulsarClient, topic);
    final Utils.SharedListenerStub listener1 = new Utils.SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumers");
    final Utils.SharedListenerStub listener2 = new Utils.SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumers");
    final Utils.SharedListenerStub listener3 = new Utils.SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumers");
    listener1.startListening();
    listener2.startListening();
    listener3.startListening();

    // when
    producer.produce(quantity);

    // then
    await() //
        .pollDelay(Duration.ofSeconds(1L))//
        .atMost(Duration.ofSeconds(10)) //
        .pollInterval(Duration.ofMillis(100)) //
        .untilAsserted(() -> assertThat(listener1.consumedCount() + listener2.consumedCount() + listener3.consumedCount()).isEqualTo(quantity));
  }

  @Test
  void produceToExclusiveConsumer() throws Exception {
    // given
    final String topic = "produceToExclusiveConsumer";
    final int quantity = 10;
    final Utils.SimpleProducerStub producer = new Utils.SimpleProducerStub(pulsarClient, topic);
    final Utils.ExclusiveListenerStub listener1 = new Utils.ExclusiveListenerStub(pulsarClient, topic, "subscription-produceToExclusiveConsumer");
    listener1.startListening();

    // when
    assertThatThrownBy(() -> new Utils.ExclusiveListenerStub(pulsarClient, "produceToExclusiveConsumer", "subscription-produceToExclusiveConsumer")) //
        .isInstanceOf(PulsarClientException.ConsumerBusyException.class) //
        .hasMessageContaining("Exclusive consumer is already connected");
    producer.produce(quantity);

    // then
    await() //
        .pollDelay(Duration.ofSeconds(1L))//
        .atMost(Duration.ofSeconds(10)) //
        .pollInterval(Duration.ofMillis(100)) //
        .untilAsserted(() -> assertThat(listener1.consumedCount()).isEqualTo(quantity));
  }

  @Test
  void produceToMultipleKeySharedConsumers() throws Exception {
    // given
    final String topic = "produceToMultipleKeySharedConsumers";
    final int quantity = 12;
    final List<Utils.KeySharedListenerStub> listeners = List.of( //
        new Utils.KeySharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleKeySharedConsumers"),
        new Utils.KeySharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleKeySharedConsumers"),
        new Utils.KeySharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleKeySharedConsumers"));

    final Utils.KeyedProducerStub producer = new Utils.KeyedProducerStub(pulsarClient, topic, (i) -> i % listeners.size());

    listeners.forEach(Utils.KeySharedListenerStub::startListening);

    // when
    final Set<String> producedKeys = producer.produce(quantity);

    // then
    assertThat(producedKeys.size()).isEqualTo(listeners.size());

    await() //
        .pollDelay(Duration.ofSeconds(1L))//
        .atMost(Duration.ofSeconds(10)) //
        .pollInterval(Duration.ofMillis(100)) //
        .untilAsserted(() -> {
          assertThat(listeners.stream().map(Utils.KeySharedListenerStub::consumedCount).reduce(0, Integer::sum)).isEqualTo(quantity);
          assertThat(listeners.stream().map(l -> l.keys().size()).reduce(0, Integer::sum)).isEqualTo(listeners.size());
        });

    for (final String key : producedKeys) {
      listeners.forEach(listener -> {
        final List<String> values = listener.forKey(key);
        if (values != null) {
          assertThat(values.size()).isEqualTo(4);
        }
      });
    }
  }

  @Test
  void produceToMultipleSharedConsumersAndOneGoesDown() throws Exception {
    // given
    final String topic = "produceToMultipleSharedConsumersAndOneGoesDown";
    final int quantity = 10;
    final Utils.SimpleProducerStub producer = new Utils.SimpleProducerStub(pulsarClient, topic);
    final Utils.SharedListenerStub listener1 = new Utils.SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumersAndOneGoesDown");
    final Utils.SharedListenerStub listener2 = new Utils.SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumersAndOneGoesDown");
    final Utils.SharedListenerStub listener3 = new Utils.SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumersAndOneGoesDown");
    listener1.startListening();
    listener2.startListening();
    listener3.startListening();

    // when
    producer.produceAsync(quantity);
    Utils.sleep(100);
    listener2.close();

    // then
    await() //
        .pollDelay(Duration.ofSeconds(1L))//
        .atMost(Duration.ofSeconds(10)) //
        .pollInterval(Duration.ofMillis(100)) //
        .untilAsserted(() -> assertThat(listener1.consumedCount() + listener2.consumedCount() + listener3.consumedCount()).isEqualTo(quantity));
  }
}
