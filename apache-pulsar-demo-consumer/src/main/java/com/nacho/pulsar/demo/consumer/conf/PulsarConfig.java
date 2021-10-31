package com.nacho.pulsar.demo.consumer.conf;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConfig {

  /*
   * Global -> persistent://property/cluster/namespace/topic
   *
   * Cluster specific -> persistent://property/global/namespace/topic
   */

  private static final String TOPIC = "persistent://sample/pulsar/ns1/my-topic";

  private static final String PULSAR_URL = "pulsar://localhost:6650";

  private static final String SUBSCRIPTION_NAME = "the-subscription";

  @Bean
  PulsarClient pulsarClient() throws PulsarClientException {
    return PulsarClient.create(PULSAR_URL);
  }

  @Bean
  Consumer pulsarConsumer(final PulsarClient pulsarClient) throws PulsarClientException {
    final var config = new ConsumerConfiguration();
    config.setSubscriptionType(SubscriptionType.Shared);
    config.setReceiverQueueSize(10);
    return pulsarClient.subscribe(TOPIC, SUBSCRIPTION_NAME, config);
  }
}
