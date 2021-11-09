package com.nacho.pulsar.demo.consumer.avro;

import com.nacho.pulsar.demo.consumer.Utils;
import com.nacho.pulsar.demo.schema.UserSchemaV2;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.UUID;

public class SimpleAvroProducerV2Stub {

  private final Producer<UserSchemaV2> producer;

  public SimpleAvroProducerV2Stub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    this.producer = Utils.avroProducerV2(pulsarClient, topic);
  }

  public void produce(String name) throws Exception {
    UserSchemaV2 userSchema = UserSchemaV2.newBuilder()
            .setId(UUID.randomUUID())
            .setUserName(name)
            .setEmail(String.format("%s@%s", name, name))
            .build();
    producer.newMessage().value(userSchema).send();
    Utils.sleep(100);
  }
}
