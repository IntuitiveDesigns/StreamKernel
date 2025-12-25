/*
 * Copyright 2025 Steven Lopez
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package com.example.streamkernel.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Future;

public class StreamKernelProducer {
    private static final Logger log = LoggerFactory.getLogger(StreamKernelProducer.class);
    private static final String PROPERTIES_FILE = "producer.properties";
    private final KafkaProducer<String, String> producer;

    public StreamKernelProducer() {
        Properties props = loadProperties();
        // We must ensure serializers are set (in case they weren't in the file)
        props.putIfAbsent("key.serializer", StringSerializer.class.getName());
        props.putIfAbsent("value.serializer", StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    private Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (input == null) {
                throw new RuntimeException("Sorry, unable to find " + PROPERTIES_FILE);
            }
            props.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("Error loading Kafka config", ex);
        }
        return props;
    }

    public void sendMessage(String topic, String key, String value) {
        long startTime = System.nanoTime();

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            Future<RecordMetadata> future = producer.send(record);

            // Wait for acknowledgement (since we set acks=all for safety)
            RecordMetadata metadata = future.get();

            long endTime = System.nanoTime();
            log.info("✅ Sent to [{}] partition {} @ offset {}", metadata.topic(), metadata.partition(), metadata.offset());

        } catch (Exception e) {
            log.error("❌ Error sending message", e);
            e.printStackTrace();
        }
    }

    public void close() {
        producer.close();
    }

    // ---------------------------------------------------------
    // MAIN METHOD: Run this to test your NVMe Broker connection
    // ---------------------------------------------------------
    public static void main(String[] args) {
        System.out.println("⚔️ Connecting to StreamKernel NVMe Broker...");

        StreamKernelProducer streamkernelProducer = new StreamKernelProducer();

        // Send 10 test messages
        for (int i = 0; i < 10; i++) {
            streamkernelProducer.sendMessage("streamkernel-test-topic", "id-" + i, "Algorithm Benchmark Result #" + i);
        }

        streamkernelProducer.close();
        System.out.println("🏁 Connection Closed.");
    }
}