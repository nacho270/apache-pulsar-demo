package com.nacho.pulsar.demo.consumer.json;

import com.nacho.pulsar.demo.consumer.Utils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.UUID;

public class SimpleJsonProducerStub {

  private final Producer<UserJson> producer;


  public SimpleJsonProducerStub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    this.producer = Utils.jsonProducer(pulsarClient, topic, UserJson.class);
  }

  public void produce(String name) throws Exception {
    UserJson user = UserJson.builder()
            .id(UUID.randomUUID())
            .name(name)
            .email(String.format("%s@%s", name, name))
            .build();
    producer.newMessage().value(user).send();
    Utils.sleep(100);
  }
}
