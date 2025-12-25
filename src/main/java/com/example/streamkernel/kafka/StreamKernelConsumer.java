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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * StreamKernelConsumer
 *
 * A lightweight standalone Kafka consumer used for:
 *  - Validating that sinks (KafkaSink / KafkaAvroSink) are writing correctly
 *  - Measuring end-to-end consumer throughput (EPS) from a topic
 *
 * This is intentionally NOT part of the core pipeline engine. It is an
 * external harness/tool that you can run alongside the pipeline.
 */
public class StreamKernelConsumer {

    private static final Logger log = LoggerFactory.getLogger(StreamKernelConsumer.class);
    private static final String PROPERTIES_FILE = "consumer.properties";

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public StreamKernelConsumer(String topic) {
        Properties props = loadProperties();

        // Ensure deserializers are present even if omitted in file
        props.putIfAbsent("key.deserializer", StringDeserializer.class.getName());
        props.putIfAbsent("value.deserializer", StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "streamkernel-consumer-shutdown"));
    }

    private Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input =
                     getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (input == null) {
                throw new IllegalStateException("Unable to find " + PROPERTIES_FILE +
                        " on the classpath. Create it under src/main/resources.");
            }
            props.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("Error loading consumer config from " + PROPERTIES_FILE, ex);
        }
        return props;
    }

    public void run() {
        log.info("🏁 StreamKernelConsumer started. Waiting for records...");

        long windowStartNs = System.nanoTime();
        long windowCount = 0;
        long totalCount = 0;

        while (running.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(500));

            if (records.isEmpty()) {
                maybeLogWindow(windowStartNs, windowCount, totalCount);
                continue;
            }

            for (ConsumerRecord<String, String> record : records) {
                windowCount++;
                totalCount++;

                // You can inspect payloads here if needed:
                // log.debug("Got record key={} value={} partition={} offset={}",
                //          record.key(), record.value(), record.partition(), record.offset());
            }

            maybeLogWindow(windowStartNs, windowCount, totalCount);
            windowStartNs = System.nanoTime();
            windowCount = 0;
        }

        consumer.close();
        log.info("✅ StreamKernelConsumer stopped. Total records consumed = {}", totalCount);
    }

    private void maybeLogWindow(long windowStartNs, long windowCount, long totalCount) {
        long now = System.nanoTime();
        long elapsedNs = now - windowStartNs;

        if (elapsedNs <= 0 || windowCount == 0) {
            return;
        }

        double seconds = elapsedNs / 1_000_000_000.0;
        double eps = windowCount / seconds;

        // Optional: compute simple per-partition lag snapshot
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
        Map<TopicPartition, Long> currentPositions = new HashMap<>();
        for (TopicPartition tp : consumer.assignment()) {
            currentPositions.put(tp, consumer.position(tp));
        }

        long totalLag = 0L;
        for (TopicPartition tp : endOffsets.keySet()) {
            long end = endOffsets.getOrDefault(tp, 0L);
            long pos = currentPositions.getOrDefault(tp, 0L);
            totalLag += Math.max(0, end - pos);
        }

        log.info("📥 Consumed {} records in ~{}s → ~{} EPS (total={} , lag={})",
                windowCount,
                String.format("%.3f", seconds),
                String.format("%.0f", eps),
                totalCount,
                totalLag
        );
    }

    private void shutdown() {
        log.info("Shutdown requested for StreamKernelConsumer...");
        running.set(false);
    }

    // ---------------------------------------------------------
    // MAIN ENTRYPOINT
    // ---------------------------------------------------------
    public static void main(String[] args) {
        String topic = (args.length > 0) ? args[0] : "streamkernel-default";
        StreamKernelConsumer consumer = new StreamKernelConsumer(topic);
        consumer.run();
    }
}
