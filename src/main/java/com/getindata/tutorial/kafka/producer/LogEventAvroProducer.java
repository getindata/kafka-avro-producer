package com.getindata.tutorial.kafka.producer;

import com.getindata.tutorial.avro.LogEvent;
import com.getindata.tutorial.generator.LogEventStreamFactory;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class LogEventAvroProducer {

  private final static Logger LOG = LoggerFactory.getLogger(LogEventAvroProducer.class);

  public static void main(String[] args) throws InterruptedException {
    if (args.length != 3) {
      LOG.error("Usage: ./run.sh <kafka_broker> <schema_registry> <kafka_topic>");
      System.exit(1);
    }

    final Producer<String, LogEvent> producer = new KafkaProducer<>(getProperties(args[0], args[1]));
    final String topic = args[2];

    registerShutdownHook(producer);

    LogEventStreamFactory.get().forEach(event -> {
      LOG.info("Event: " + event.toString());

      // use server name as key, so events from the same server will land on the same Kafka partition
      ProducerRecord<String, LogEvent> record =
          new ProducerRecord<>(topic, event.getServer().toString(), event);

      // send record asynchronously
      producer.send(record, LogEventAvroProducer::sendCallback);
    });

  }

  private static Properties getProperties(final String broker, final String schemaRegistry) {
    final Properties props = new Properties();

    // Kafka broker address
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

    // how many ACKs to receive before assuming request is completed
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    // how many times resend record in case of failure
    props.put(ProducerConfig.RETRIES_CONFIG, 0);

    // Avro serializers for key and value of record
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);

    // Avro serializer property - address of Schema Registry
    props.put("schema.registry.url", "http://" + schemaRegistry);
    return props;
  }

  private static void registerShutdownHook(final Producer<String, LogEvent> producer) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        LOG.debug("Closing application...");

        // remember to free resources allocated by Kafka producer
        producer.close(2, TimeUnit.SECONDS);

        LOG.info("Application closed.");
      }
    });
  }

  private static void sendCallback(RecordMetadata metadata, Exception exception) {
    // XXX: this callback is called in I/O thread of producer, so consider using own Executor
    if (exception != null) {
      LOG.error("Failure while sending record: " + exception);
    } else {
      LOG.info("Offset of the record we just send is  " + metadata.offset());
    }
  }

}

