package com.nacho.pulsar.demo.consumer.avro.object;

import com.nacho.pulsar.demo.consumer.Utils;
import com.nacho.pulsar.demo.schema.UserSchemaV2;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.UUID;

public class SimpleAvroObjectProducerV2Stub {

  private final Producer<Object> producer;

  public SimpleAvroObjectProducerV2Stub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    this.producer = Utils.objectProducerV2(pulsarClient, topic);
  }

  public void produce(String name) throws Exception {
    UserSchemaV2 userSchema = UserSchemaV2.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setUserName(name)
            .setEmail(String.format("%s@%s", name, name))
            .build();
    producer.newMessage().value(userSchema).send();
    Utils.sleep(100);
  }
}
