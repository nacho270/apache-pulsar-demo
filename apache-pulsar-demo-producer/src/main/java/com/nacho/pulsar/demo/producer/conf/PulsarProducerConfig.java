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
   * Global -> persistent://property/cluster/namespace/topic
   *
   * Cluster specific -> persistent://property/global/namespace/topic
   */

  private static final String TOPIC = "persistent://sample/pulsar/ns1/my-topic";

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
  Producer<String> pulsarProducer(final PulsarClient pulsarClient) throws PulsarClientException {
    return pulsarClient //
        .newProducer(Schema.STRING) //
        .topic(TOPIC) //
        .create();
  }
}