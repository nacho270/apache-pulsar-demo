package com.nacho.pulsar.demo.consumer;

import com.nacho.pulsar.demo.consumer.avro.SimpleAvroProducerV1Stub;
import com.nacho.pulsar.demo.consumer.avro.SimpleAvroProducerV2Stub;
import com.nacho.pulsar.demo.consumer.avro.SimpleAvroV2ListenerStub;
import com.nacho.pulsar.demo.consumer.bytearray.SimpleByteArrayListenerV2Stub;
import com.nacho.pulsar.demo.consumer.bytearray.SimpleByteArrayProducerV1Stub;
import com.nacho.pulsar.demo.consumer.bytearray.SimpleByteArrayProducerV2Stub;
import com.nacho.pulsar.demo.consumer.json.SimpleJsonListenerStub;
import com.nacho.pulsar.demo.consumer.json.SimpleJsonProducerStub;
import com.nacho.pulsar.demo.consumer.modes.ExclusiveListenerStub;
import com.nacho.pulsar.demo.consumer.modes.KeySharedListenerStub;
import com.nacho.pulsar.demo.consumer.modes.KeyedProducerStub;
import com.nacho.pulsar.demo.consumer.modes.SharedListenerStub;
import com.nacho.pulsar.demo.consumer.modes.SimpleProducerStub;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
class TestCases {

  @Container
  private static final PulsarContainer pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:latest"));

  private static PulsarClient pulsarClient;

  @BeforeAll
  public static void init() throws PulsarClientException, PulsarAdminException {
    pulsarClient = PulsarClient.builder() //
            .serviceUrl(pulsarContainer.getPulsarBrokerUrl()) //
            .build();
    PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.getHttpServiceUrl()).build();
    for (String tenant : pulsarAdmin.tenants().getTenants()) {
      for (String namespace : pulsarAdmin.namespaces().getNamespaces(tenant)) {
        pulsarAdmin.namespaces().setIsAllowAutoUpdateSchema(namespace, true);
//        pulsarAdmin.namespaces().setSchemaCompatibilityStrategy(namespace, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
//        pulsarAdmin.namespaces().setSchemaValidationEnforced(namespace, false);
      }
    }
  }

  @Test
  void produceToSingleSharedConsumer() throws Exception {
    // given
    final String topic = "produceToSingleSharedConsumer";
    final int quantity = 10;
    final SimpleProducerStub producer = new SimpleProducerStub(pulsarClient, topic);
    final SharedListenerStub listener = new SharedListenerStub(pulsarClient, topic, "subscription-produceToSingleSharedConsumer");
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
    final SimpleProducerStub producer = new SimpleProducerStub(pulsarClient, topic);
    final SharedListenerStub listener1 = new SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumers");
    final SharedListenerStub listener2 = new SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumers");
    final SharedListenerStub listener3 = new SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumers");
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
    final SimpleProducerStub producer = new SimpleProducerStub(pulsarClient, topic);
    final ExclusiveListenerStub listener1 = new ExclusiveListenerStub(pulsarClient, topic, "subscription-produceToExclusiveConsumer");
    listener1.startListening();

    // when
    assertThatThrownBy(() -> new ExclusiveListenerStub(pulsarClient, "produceToExclusiveConsumer", "subscription-produceToExclusiveConsumer")) //
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
    final List<KeySharedListenerStub> listeners = List.of( //
            new KeySharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleKeySharedConsumers"),
            new KeySharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleKeySharedConsumers"),
            new KeySharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleKeySharedConsumers"));

    final KeyedProducerStub producer = new KeyedProducerStub(pulsarClient, topic, (i) -> i % listeners.size());

    listeners.forEach(KeySharedListenerStub::startListening);

    // when
    final Set<String> producedKeys = producer.produce(quantity);

    // then
    assertThat(producedKeys.size()).isEqualTo(listeners.size());

    await() //
            .pollDelay(Duration.ofSeconds(1L))//
            .atMost(Duration.ofSeconds(10)) //
            .pollInterval(Duration.ofMillis(100)) //
            .untilAsserted(() -> {
              assertThat(listeners.stream().map(KeySharedListenerStub::consumedCount).reduce(0, Integer::sum)).isEqualTo(quantity);
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
    final SimpleProducerStub producer = new SimpleProducerStub(pulsarClient, topic);
    final SharedListenerStub listener1 = new SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumersAndOneGoesDown");
    final SharedListenerStub listener2 = new SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumersAndOneGoesDown");
    final SharedListenerStub listener3 = new SharedListenerStub(pulsarClient, topic, "subscription-produceToMultipleSharedConsumersAndOneGoesDown");
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

  @Test
  void produceAndConsumeByteArraySameVersion() throws Exception {
    // given
    final String topic = "produceAndConsumeByteArraySameVersion";
    final SimpleByteArrayProducerV2Stub producerV2 = new SimpleByteArrayProducerV2Stub(pulsarClient, topic);
    final SimpleByteArrayListenerV2Stub listenerV2 = new SimpleByteArrayListenerV2Stub(pulsarClient, topic, "subscription-produceAndConsumeByteArraySameVersion");
    listenerV2.startListening();

    // when
    producerV2.produce("userV2");

    // then
    await() //
            .pollDelay(Duration.ofSeconds(1L))//
            .atMost(Duration.ofSeconds(10)) //
            .pollInterval(Duration.ofMillis(100)) //
            .untilAsserted(() -> assertThat(listenerV2.consumedCount()).isEqualTo(1));
  }

  @Test
  void produceAndConsumeJson() throws Exception {
    // given
    final String topic = "produceAndConsumeJson";
    final SimpleJsonProducerStub producer = new SimpleJsonProducerStub(pulsarClient, topic);
    final SimpleJsonListenerStub listener = new SimpleJsonListenerStub(pulsarClient, topic, "subscription-produceAndConsumeJson");
    listener.startListening();

    // when
    producer.produce("user");

    // then
    await() //
            .pollDelay(Duration.ofSeconds(1L))//
            .atMost(Duration.ofSeconds(10)) //
            .pollInterval(Duration.ofMillis(100)) //
            .untilAsserted(() -> assertThat(listener.consumedCount()).isEqualTo(1));
  }

  @Test
  void produceAndConsumeAvroSameVersion() throws Exception {
    // given
    final String topic = "produceAndConsumeAvro";
    final SimpleAvroProducerV2Stub producerV2 = new SimpleAvroProducerV2Stub(pulsarClient, topic);
    final SimpleAvroV2ListenerStub listenerV2 = new SimpleAvroV2ListenerStub(pulsarClient, topic, "subscription-SameVersion");
    listenerV2.startListening();

    // when
    producerV2.produce("userV2");

    // then
    await() //
            .pollDelay(Duration.ofSeconds(1L))//
            .atMost(Duration.ofSeconds(10)) //
            .pollInterval(Duration.ofMillis(100)) //
            .untilAsserted(() -> assertThat(listenerV2.consumedCount()).isEqualTo(1));
  }

  // There maybe don't make sense. We'll send the same schema but different fields. Here i'm sending different schemas
  @Test
  @Ignore
  void produceAndConsumeByteArrayDifferentVersion() throws Exception {
    // given
    final String topic = "produceAndConsumeByteArrayDifferentVersion";
    final SimpleByteArrayProducerV1Stub producerV1 = new SimpleByteArrayProducerV1Stub(pulsarClient, topic);
    final SimpleByteArrayProducerV2Stub producerV2 = new SimpleByteArrayProducerV2Stub(pulsarClient, topic);
    final SimpleByteArrayListenerV2Stub listenerV2 = new SimpleByteArrayListenerV2Stub(pulsarClient, topic, "subscription-produceAndConsumeByteArrayDifferentVersion");
    listenerV2.startListening();

    // when
    producerV1.produce("userV1");
    producerV2.produce("userV2");

    // then
    await() //
            .pollDelay(Duration.ofSeconds(1L))//
            .atMost(Duration.ofSeconds(10)) //
            .pollInterval(Duration.ofMillis(100)) //
            .untilAsserted(() -> assertThat(listenerV2.consumedCount()).isEqualTo(1));
  }

  @Test
  @Ignore
  void produceAndConsumeAvroDifferentVersion() throws Exception {
    // given
    final String topic = "produceAndConsumeAvroDifferentVersion";
    final SimpleAvroProducerV1Stub producerV1 = new SimpleAvroProducerV1Stub(pulsarClient, topic);
    final SimpleAvroProducerV2Stub producerV2 = new SimpleAvroProducerV2Stub(pulsarClient, topic);
    final SimpleAvroV2ListenerStub listenerV2 = new SimpleAvroV2ListenerStub(pulsarClient, topic, "subscription-produceAndConsumeAvroDifferentVersion");
    listenerV2.startListening();

    // when
    producerV1.produce("userV1");
    producerV2.produce("userV2");

    // then
    await() //
            .pollDelay(Duration.ofSeconds(1L))//
            .atMost(Duration.ofSeconds(10)) //
            .pollInterval(Duration.ofMillis(100)) //
            .untilAsserted(() -> assertThat(listenerV2.consumedCount()).isEqualTo(2));
  }
}
