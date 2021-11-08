package com.nacho.pulsar.demo.producer.conf;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarProducerConfig {

  /*
   * Global -> persistent://domain/global/namespace/topic
   *
   * Cluster specific -> persistent://domain/tenant/namespace/topic.
   */

  private static final String KEY_SHARED_TOPIC = "persistent://sample/pulsar/ns1/key_shared-topic";

  private static final String SHARED_TOPIC = "persistent://sample/pulsar/ns1/shared-topic";

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
  Producer<String> pulsarKeySharedProducer(final PulsarClient pulsarClient) throws PulsarClientException {
    return pulsarClient //
        .newProducer(Schema.STRING) //
        .topic(KEY_SHARED_TOPIC) //
        .create();
  }

  @Bean
  Producer<String> pulsarSharedProducer(final PulsarClient pulsarClient) throws PulsarClientException {
    return pulsarClient //
        .newProducer(Schema.STRING) //
        .topic(SHARED_TOPIC) //
        .create();
  }
}
