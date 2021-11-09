package com.nacho.pulsar.demo.consumer.bytearray;

import com.nacho.pulsar.demo.consumer.Utils;
import com.nacho.pulsar.demo.schema.UserSchema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.io.ByteArrayOutputStream;
import java.util.UUID;

public class SimpleByteArrayProducerV1Stub {

  private final Producer<byte[]> producer;

  private static final SpecificDatumWriter<UserSchema> SPECIFIC_DATUM_WRITER = new SpecificDatumWriter<>(UserSchema.getClassSchema());

  public SimpleByteArrayProducerV1Stub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    this.producer = Utils.byteProducer(pulsarClient, topic);
  }

  public void produce(String name) throws Exception {
    UserSchema user = UserSchema.newBuilder()
            .setId(UUID.randomUUID())
            .setUserName(name)
            .build();
    try (var serializedUser = new ByteArrayOutputStream()) {
      var encoder = EncoderFactory.get().jsonEncoder(UserSchema.getClassSchema(), serializedUser, false);
      SPECIFIC_DATUM_WRITER.write(user, encoder);
      encoder.flush();
      producer.newMessage().value(serializedUser.toByteArray()).send();
      Utils.sleep(100);
    }
  }
}
