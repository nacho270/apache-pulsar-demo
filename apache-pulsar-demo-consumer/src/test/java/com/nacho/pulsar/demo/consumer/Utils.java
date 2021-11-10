package com.nacho.pulsar.demo.consumer;

import com.nacho.pulsar.demo.schema.UserSchema;
import com.nacho.pulsar.demo.schema.UserSchemaV2;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;

import java.util.concurrent.TimeUnit;

@UtilityClass
public class Utils {

  public static Consumer<String> consumer(final PulsarClient pulsarClient, final String topic, final String subscription,
                                          final SubscriptionType subscriptionType) throws PulsarClientException {
    return pulsarClient //
            .newConsumer(Schema.STRING) //
            .topic(topic) //
            .subscriptionName(subscription) //
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) //
            .subscriptionType(subscriptionType) //
            .receiverQueueSize(10) //
            .deadLetterPolicy(DeadLetterPolicy.builder()
                    .deadLetterTopic("the-dlt-topic")
                    .retryLetterTopic("the-retry-topic")
                    .maxRedeliverCount(10)
                    .build())
            .subscribe();
  }

  public static Producer<String> producer(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    return pulsarClient //
            .newProducer(Schema.STRING) //
            .topic(topic) //
            .create();
  }

  /******************************************************************************************************************************************************************************************************************/

  public static Producer<UserSchema> avroProducerV1(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    return pulsarClient //
            .newProducer(AvroSchema.of(UserSchema.class)) //
            .topic(topic) //
            .create();
  }

  public static Producer<UserSchemaV2> avroProducerV2(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    return pulsarClient //
            .newProducer(AvroSchema.of(UserSchemaV2.class)) //
            .topic(topic) //
            .create();
  }

  public static Consumer<UserSchemaV2> avroConsumerV2(final PulsarClient pulsarClient, final String topic,
                                                      final String subscription, final SubscriptionType subscriptionType) throws PulsarClientException {
    return pulsarClient //
            .newConsumer(AvroSchema.of(UserSchemaV2.class)) //
            .topic(topic) //
            .subscriptionName(subscription) //
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) //
            .subscriptionType(subscriptionType) //
            .receiverQueueSize(10) //
            .subscribe();
  }

  /******************************************************************************************************************************************************************************************************************/

  public static Producer<Object> objectProducerV1(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    SchemaDefinition<Object> schema = SchemaDefinition.builder().withJsonDef("""
            {
              "type": "record",
              "name": "UserSchema",
              "namespace": "com.nacho.pulsar.demo.schema",
              "fields": [
                {
                  "name": "id",
                  "type": {
                    "type": "string"
                  }
                },
                {
                  "name": "userName",
                  "type": {
                    "type": "string"
                  }
                }
              ]
            }
            """).build();

    return pulsarClient //
            .newProducer(Schema.AVRO(schema)) //
            .topic(topic) //
            .create();
  }

  public static Producer<Object> objectProducerV2(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    SchemaDefinition<Object> schema = SchemaDefinition.builder().withJsonDef("""
            {
              "type": "record",
              "name": "UserSchema",
              "namespace": "com.nacho.pulsar.demo.schema",
              "fields": [
                   {
                     "name": "id",
                     "type": {
                       "type": "string"
                     }
                   },
                   {
                     "name": "userName",
                     "type": {
                       "type": "string"
                     }
                   },
                   {
                     "name": "email",
                     "type": [
                       "null",
                       "string"
                     ],
                     "default": null
                   }
                 ]
            }
            """)
            .withAlwaysAllowNull(true)
            .withSupportSchemaVersioning(true)
            .build();

    return pulsarClient //
            .newProducer(Schema.AVRO(schema)) //
            .topic(topic) //
            .create();
  }

  public static Consumer<Object> objectConsumerV2(final PulsarClient pulsarClient, final String topic,
                                                  final String subscription, final SubscriptionType subscriptionType) throws PulsarClientException {
    SchemaDefinition<Object> schema = SchemaDefinition.builder().withJsonDef("""
            {
              "type": "record",
              "name": "UserSchema",
              "namespace": "com.nacho.pulsar.demo.schema",
              "fields": [
                   {
                     "name": "id",
                     "type": {
                       "type": "string"
                     }
                   },
                   {
                     "name": "userName",
                     "type": {
                       "type": "string"
                     }
                   },
                   {
                     "name": "email",
                     "type": [
                       "null",
                       "string"
                     ],
                     "default": null
                   }
                 ]
            }
            """)
            .withAlwaysAllowNull(true)
            .withSupportSchemaVersioning(true)
            .build();
    return pulsarClient //
            .newConsumer(Schema.AVRO(schema)) //
            .topic(topic) //
            .subscriptionName(subscription) //
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) //
            .subscriptionType(subscriptionType) //
            .receiverQueueSize(10) //
            .subscribe();
  }


  /******************************************************************************************************************************************************************************************************************/

  public static Producer<byte[]> byteProducer(final PulsarClient pulsarClient, final String topic) throws PulsarClientException {
    return pulsarClient //
            .newProducer() //
            .topic(topic) //
            .create();
  }


  public static Consumer<byte[]> byteConsumer(final PulsarClient pulsarClient, final String topic, final String subscription,
                                              final SubscriptionType subscriptionType) throws PulsarClientException {
    return pulsarClient //
            .newConsumer() //
            .topic(topic) //
            .subscriptionName(subscription) //
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) //
            .subscriptionType(subscriptionType) //
            .receiverQueueSize(10) //
            .subscribe();
  }

  /******************************************************************************************************************************************************************************************************************/

  public static <T> Producer<T> jsonProducer(PulsarClient pulsarClient, String topic, Class<T> clazz) throws PulsarClientException {
    return pulsarClient //
            .newProducer(Schema.JSON(clazz)) //
            .topic(topic) //
            .create();
  }


  public static <T> Consumer<T> jsonConsumer(final PulsarClient pulsarClient, final String topic, final String subscription,
                                             final SubscriptionType subscriptionType, Class<T> clazz) throws PulsarClientException {
    return pulsarClient //
            .newConsumer(Schema.JSON(clazz)) //
            .topic(topic) //
            .subscriptionName(subscription) //
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) //
            .subscriptionType(subscriptionType) //
            .receiverQueueSize(10) //
            .subscribe();
  }


  public static void sleep(final long millis) throws Exception {
    TimeUnit.MILLISECONDS.sleep(millis);
  }
}
