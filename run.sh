#!/bin/bash

java -cp target/uber-kafka-avro-producer-1.0-SNAPSHOT.jar com.getindata.tutorial.kafka.producer.LogEventAvroProducer $*
