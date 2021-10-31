package com.nacho.pulsar.demo.producer.conf;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
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

  @Bean
  PulsarClient pulsarClient() throws PulsarClientException {
    return PulsarClient.create(PULSAR_URL);
  }

  @Bean
  Producer pulsarProducer(final PulsarClient pulsarClient) throws PulsarClientException {
    return pulsarClient.createProducer(TOPIC);
  }
}
