package com.nacho.pulsar.demo.consumer.bytearray;

import com.nacho.pulsar.demo.consumer.Utils;
import com.nacho.pulsar.demo.schema.UserSchemaV2;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.io.ByteArrayOutputStream;
import java.util.UUID;

public class SimpleByteArrayProducerV2Stub {

  private final Producer<byte[]> producer;

  private static final SpecificDatumWriter<UserSchemaV2> SPECIFIC_DATUM_WRITER = new SpecificDatumWriter<>(UserSchemaV2.getClassSchema());

  public SimpleByteArrayProducerV2Stub(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    this.producer = Utils.byteProducer(pulsarClient, topic);
  }

  public void produce(String name) throws Exception {
    UserSchemaV2 user = UserSchemaV2.newBuilder()
            .setId(UUID.randomUUID())
            .setUserName(name)
            .setEmail(String.format("%s@%s", name, name))
            .build();
    try (var serializedUser = new ByteArrayOutputStream()) {
      var encoder = EncoderFactory.get().jsonEncoder(UserSchemaV2.getClassSchema(), serializedUser, false);
      SPECIFIC_DATUM_WRITER.write(user, encoder);
      encoder.flush();
      producer.newMessage().value(serializedUser.toByteArray()).send();
      Utils.sleep(100);
    }
  }
}
