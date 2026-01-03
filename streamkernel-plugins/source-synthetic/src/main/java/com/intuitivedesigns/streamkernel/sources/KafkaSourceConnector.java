/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.sources;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * High-Performance Kafka Source.
 * Features:
 * - Deterministic Payload IDs (Ensures Idempotency)
 * - Metrics Integration
 * - Secure Config Propagation
 */
public final class KafkaSourceConnector implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceConnector.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final Queue<PipelinePayload<String>> buffer = new ConcurrentLinkedQueue<>();
    private final MetricsRuntime metrics;

    // Config
    private final Duration pollDuration;

    public KafkaSourceConnector(String topic, Properties props, MetricsRuntime metrics, Duration pollDuration) {
        this.topic = topic;
        this.metrics = metrics;
        this.pollDuration = pollDuration;

        // Load consumer
        Thread.currentThread().setContextClassLoader(null); // Safety hack for some environments
        this.consumer = new KafkaConsumer<>(props);
    }

    public static KafkaSourceConnector fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        String topic = config.getString("source.kafka.topic", null);
        Objects.requireNonNull(topic, "Missing config: source.kafka.topic");

        Properties props = buildConsumerProps(config);

        // Tune poll duration (lower = tighter loops, higher = less CPU when idle)
        long pollMs = config.getLong("source.kafka.poll.ms", 100L);

        return new KafkaSourceConnector(topic, props, metrics, Duration.ofMillis(pollMs));
    }

    private static Properties buildConsumerProps(PipelineConfig config) {
        Properties props = new Properties();

        // 1. Connection
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.broker", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumer.group", "streamkernel-group"));

        // 2. Deserialization
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 3. Behavior
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("kafka.consumer.auto.offset.reset", "latest"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getString("kafka.consumer.auto.commit", "true"));

        // 4. Performance Tuning
        // Fetch more data per request to reduce network RTT
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getString("kafka.consumer.max.poll.records", "500"));
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, config.getString("kafka.consumer.fetch.min.bytes", "1"));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, config.getString("kafka.consumer.fetch.max.wait.ms", "500"));

        // 5. Security Passthrough
        for (String key : config.keys()) {
            if (key.startsWith("kafka.ssl.") || key.startsWith("kafka.security.") || key.startsWith("kafka.sasl.")) {
                props.put(key.substring(6), config.getString(key, ""));
            }
        }
        return props;
    }

    @Override
    public void connect() {
        log.info("🔌 Connecting to Kafka Topic: {}", topic);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void disconnect() {
        log.info("🔌 Disconnecting Kafka Consumer...");
        try {
            consumer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("Error closing consumer", e);
        }
    }

    @Override
    public PipelinePayload<String> fetch() {
        // 1. Drain Buffer first
        if (!buffer.isEmpty()) {
            return buffer.poll();
        }

        // 2. Poll Kafka if buffer empty
        try {
            ConsumerRecords<String, String> records = consumer.poll(pollDuration);

            if (records.isEmpty()) return null; // Yield to kernel

            int count = 0;
            for (ConsumerRecord<String, String> record : records) {
                // Metadata for provenance
                Map<String, String> meta = new HashMap<>();
                meta.put("kafka.topic", record.topic());
                meta.put("kafka.partition", String.valueOf(record.partition()));
                meta.put("kafka.offset", String.valueOf(record.offset()));
                if (record.key() != null) meta.put("kafka.key", record.key());

                // DETERMINISTIC ID GENERATION
                // Prefer Key. If null, use Partition:Offset (guaranteed unique per topic)
                String id = (record.key() != null && !record.key().isEmpty())
                        ? record.key()
                        : record.partition() + ":" + record.offset();

                buffer.add(new PipelinePayload<>(
                        id,
                        record.value(),
                        Instant.ofEpochMilli(record.timestamp()),
                        meta
                ));
                count++;
            }

            // Record Metrics
            if (metrics != null) {
                metrics.counter("source.kafka.read", count);
            }

        } catch (Exception e) {
            log.error("Kafka Poll Failed", e);
            if (metrics != null) metrics.counter("source.kafka.errors", 1.0);
            return null;
        }

        return buffer.poll();
    }
}