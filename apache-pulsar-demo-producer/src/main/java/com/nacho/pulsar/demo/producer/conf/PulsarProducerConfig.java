package com.nacho.pulsar.demo.producer.conf;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
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

  private static final String PULSAR_ADMIN_URL = "http://localhost:8180";

  private static final String PULSAR_URL = "pulsar://localhost:6650";

  @Bean
  PulsarAdmin pulsarAdmin() throws PulsarClientException {
    return PulsarAdmin.builder().serviceHttpUrl(PULSAR_ADMIN_URL).build();
  }

  @Bean
  PulsarClient pulsarClient(final PulsarAdmin pulsarAdmin) throws PulsarClientException {
    return PulsarClient.builder() //
        .serviceUrl(PULSAR_URL) //
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
