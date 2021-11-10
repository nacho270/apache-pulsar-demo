package com.nacho.pulsar.demo.consumer.avro.object;

import com.nacho.pulsar.demo.consumer.Utils;
import com.nacho.pulsar.demo.schema.UserSchema;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.UUID;

public class SimpleAvroObjectProducerV1Stub {

  private final Producer<Object> producer;

  public SimpleAvroObjectProducerV1Stub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    this.producer = Utils.objectProducerV1(pulsarClient, topic);
  }

  public void produce(String name) throws Exception {
    UserSchema userSchema = UserSchema.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setUserName(name)
            .build();
    producer.newMessage().value(userSchema).send();
    Utils.sleep(100);
  }
}
