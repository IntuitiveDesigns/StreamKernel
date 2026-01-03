/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.kafka;

import com.intuitivedesigns.streamkernel.bench.SyntheticSource;
import com.intuitivedesigns.streamkernel.core.PipelinePayload; // Import this!
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

/**
 * STRESS TEST: Floods localhost:9092 with events to measure max network throughput.
 */
public class ProducerBurnTest {

    public static void main(String[] args) {
        // 1. Force Localhost Config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Tuning for Maximum Throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072"); // 128KB batches
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // Leader-only for speed

        System.out.println("🚀 Starting NETWORK Burn Test -> localhost:9092");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            SyntheticSource source = new SyntheticSource(1024, false);
            LongAdder counter = new LongAdder();
            long start = System.currentTimeMillis();

            while (true) {
                List<PipelinePayload<String>> batch = source.fetchBatch(2000);

                for (PipelinePayload<String> payload : batch) {
                    producer.send(new ProducerRecord<>("bench-topic", payload.data()));
                }
                counter.add(batch.size());

                // Report stats every 3 seconds
                long now = System.currentTimeMillis();
                if (now - start >= 3000) {
                    double seconds = (now - start) / 1000.0;
                    long total = counter.sum();
                    System.out.printf("NETWORK SPEED: %,.0f events/sec (Total: %,d)%n", total / seconds, total);

                    start = now;
                    counter.reset();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}