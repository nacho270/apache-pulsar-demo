package com.nacho.pulsar.demo.consumer.bytearray;

import com.nacho.pulsar.demo.consumer.Utils;
import com.nacho.pulsar.demo.schema.UserSchemaV2;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SimpleByteArrayListenerV2Stub {

  private static final SpecificDatumReader<UserSchemaV2> SPECIFIC_DATUM_READER = new SpecificDatumReader<>(UserSchemaV2.getClassSchema());
  private final Consumer<byte[]> pulsarSharedConsumer;
  private final List<UserSchemaV2> consumed = new ArrayList<>();

  public SimpleByteArrayListenerV2Stub(final PulsarClient pulsarClient, final String topic, final String subscription) throws PulsarClientException {
    pulsarSharedConsumer = Utils.byteConsumer(pulsarClient, topic, subscription, SubscriptionType.Shared);
  }

  public void startListening() {
    new Thread(() -> {
      try {
        while (pulsarSharedConsumer.isConnected()) {
          final Message<byte[]> msg = pulsarSharedConsumer.receive(100, TimeUnit.MILLISECONDS);
          if (msg != null) {
            try (var deserializedUser = new ByteArrayInputStream(msg.getValue())) {
              var decoder = DecoderFactory.get().jsonDecoder(UserSchemaV2.getClassSchema(), deserializedUser);
              UserSchemaV2 userSchemaV2 = SPECIFIC_DATUM_READER.read(null, decoder);
              consumed.add(userSchemaV2);
            }
            pulsarSharedConsumer.acknowledge(msg);
          }
        }
        log.info("Consumer closed");
      } catch (final Exception e) {
        e.printStackTrace();
      }

    }).start();
  }

  public int consumedCount() {
    return consumed.size();
  }
}
