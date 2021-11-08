package com.nacho.pulsar.demo.consumer.conf;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConsumerConfig {

  /*
   * Global -> persistent://domain/global/namespace/topic
   *
   * Cluster specific -> persistent://domain/tenant/namespace/topic.
   */

  private static final String KEY_SHARED_TOPIC = "persistent://sample/pulsar/ns1/key_shared-topic";

  private static final String SHARED_TOPIC = "persistent://sample/pulsar/ns1/shared-topic";

  private static final String KEY_SHARED_SUBSCRIPTION_NAME = "key_shared-subscription";

  private static final String SHARED_SUBSCRIPTION_NAME = "shared-subscription";


  @Bean
  PulsarAdmin pulsarAdmin(@Value("${pulsar.admin}") String pulsarAdmin) throws PulsarClientException {
    return PulsarAdmin.builder().serviceHttpUrl(pulsarAdmin).build();
  }

  @Bean
  PulsarClient pulsarClient(@Value("${pulsar.url}") String pulsarUrl) throws PulsarClientException {
    return PulsarClient.builder() //
        .serviceUrl(pulsarUrl) //
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
