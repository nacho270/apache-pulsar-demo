package com.nacho.pulsar.demo.consumer.conf;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConsumerConfig {

  /*
   * Global -> persistent://property/cluster/namespace/topic
   *
   * Cluster specific -> persistent://property/global/namespace/topic
   */

  private static final String KEY_SHARED_TOPIC = "persistent://sample/pulsar/ns1/key_shared-topic";

  private static final String SHARED_TOPIC = "persistent://sample/pulsar/ns1/shared-topic";

  private static final String PULSAR_URL = "pulsar://localhost:6650";

  private static final String PULSAR_ADMIN_URL = "http://localhost:8180";

  private static final String KEY_SHARED_SUBSCRIPTION_NAME = "key_shared-subscription";

  private static final String SHARED_SUBSCRIPTION_NAME = "shared-subscription";

  @Bean
  PulsarAdmin pulsarAdmin() throws PulsarClientException {
    return PulsarAdmin.builder().serviceHttpUrl(PULSAR_ADMIN_URL).build();
  }

  @Bean
  PulsarClient pulsarClient() throws PulsarClientException {
    return PulsarClient.builder() //
        .serviceUrl(PULSAR_URL) //
        .build();
  }

  @Bean
  Consumer<String> pulsarSharedConsumer(final PulsarClient pulsarClient) throws PulsarClientException, PulsarAdminException {
    return pulsarClient //
        .newConsumer(Schema.STRING) //
        .topic(SHARED_TOPIC) //
        .subscriptionName(SHARED_SUBSCRIPTION_NAME) //
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) //
        .subscriptionType(SubscriptionType.Shared) //
        .receiverQueueSize(10) //
        .subscribe();
  }

  @Bean
  Consumer<String> pulsarKeySharedConsumer(final PulsarClient pulsarClient) throws PulsarClientException, PulsarAdminException {
    return pulsarClient //
        .newConsumer(Schema.STRING) //
        .topic(KEY_SHARED_TOPIC) //
        .subscriptionName(KEY_SHARED_SUBSCRIPTION_NAME) //
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) //
        .subscriptionType(SubscriptionType.Key_Shared) //
        .receiverQueueSize(10) //
        .subscribe();
  }
}
