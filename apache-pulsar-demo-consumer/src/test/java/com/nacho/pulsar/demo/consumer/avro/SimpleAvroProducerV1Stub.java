package com.nacho.pulsar.demo.consumer.avro;

import com.nacho.pulsar.demo.consumer.Utils;
import com.nacho.pulsar.demo.schema.UserSchema;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.UUID;

public class SimpleAvroProducerV1Stub {

  private final Producer<UserSchema> producer;

  public SimpleAvroProducerV1Stub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    this.producer = Utils.avroProducerV1(pulsarClient, topic);
  }

  public void produce(String name) throws Exception {
    UserSchema userSchema = UserSchema.newBuilder().setId(UUID.randomUUID()).setUserName(name).build();
    producer.newMessage().value(userSchema).send();
    Utils.sleep(100);
  }
}
