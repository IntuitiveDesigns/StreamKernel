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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * StreamKernelConsumer
 *
 * A lightweight standalone Kafka consumer used for:
 *  - Validating that sinks (KafkaSink / KafkaAvroSink) are writing correctly
 *  - Measuring end-to-end consumer throughput (EPS) from a topic
 *
 * Notes:
 *  - Uses a fixed reporting window (by time) for stable EPS.
 *  - Uses consumer.wakeup() for fast, safe shutdown while polling.
 *  - Computes lag at most once per report interval to avoid endOffsets() overhead per loop.
 */
public final class StreamKernelConsumer {

    private static final Logger log = LoggerFactory.getLogger(StreamKernelConsumer.class);

    private static final String PROPERTIES_FILE = "consumer.properties";

    // System properties (optional overrides)
    private static final String P_PROPERTIES_FILE = "consumer.properties.file";
    private static final String P_POLL_MS = "consumer.poll.ms";
    private static final String P_REPORT_MS = "consumer.report.ms";
    private static final String P_TOPIC_DEFAULT = "consumer.topic.default";

    // Defaults
    private static final long DEFAULT_POLL_MS = 500L;
    private static final long DEFAULT_REPORT_MS = 1_000L;
    private static final String DEFAULT_TOPIC = "streamkernel-default";

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final long pollMs;
    private final long reportMs;

    public StreamKernelConsumer(String topic) {
        Objects.requireNonNull(topic, "topic");

        final Properties props = loadProperties(resolvePropertiesFile());
        props.putIfAbsent("key.deserializer", StringDeserializer.class.getName());
        props.putIfAbsent("value.deserializer", StringDeserializer.class.getName());

        this.pollMs = clampLong(getLong(P_POLL_MS, DEFAULT_POLL_MS), 10L, 60_000L);
        this.reportMs = clampLong(getLong(P_REPORT_MS, DEFAULT_REPORT_MS), 250L, 60_000L);

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "streamkernel-consumer-shutdown"));
    }

    private static String resolvePropertiesFile() {
        final String override = System.getProperty(P_PROPERTIES_FILE);
        if (override == null) return PROPERTIES_FILE;
        final String trimmed = override.trim();
        return trimmed.isEmpty() ? PROPERTIES_FILE : trimmed;
    }

    private Properties loadProperties(String resourceName) {
        final Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            if (input == null) {
                throw new IllegalStateException("Unable to find " + resourceName +
                        " on the classpath. Create it under src/main/resources or set -D" +
                        P_PROPERTIES_FILE + "=<name>.");
            }
            props.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("Error loading consumer config from " + resourceName, ex);
        }
        return props;
    }

    public void run() {
        log.info("StreamKernelConsumer started. pollMs={} reportMs={}", pollMs, reportMs);

        long totalCount = 0L;

        long windowStartNs = System.nanoTime();
        long windowCount = 0L;
        long lastReportNs = windowStartNs;

        long lastLag = -1L;
        long lastLagSampleNs = 0L;

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records;
                try {
                    records = consumer.poll(Duration.ofMillis(pollMs));
                } catch (org.apache.kafka.common.errors.WakeupException we) {
                    break; // expected on shutdown
                }

                // OPTIMIZATION: records.count() is O(1). No need to iterate.
                if (!records.isEmpty()) {
                    int count = records.count();
                    windowCount += count;
                    totalCount += count;
                }

                final long nowNs = System.nanoTime();

                // Report Window Logic
                if (TimeUnit.NANOSECONDS.toMillis(nowNs - lastReportNs) >= reportMs) {
                    // Check Lag (Blocking call, so we gate it)
                    if (TimeUnit.NANOSECONDS.toMillis(nowNs - lastLagSampleNs) >= reportMs) {
                        // Only check lag if we actually have an assignment
                        if (consumer.assignment() != null && !consumer.assignment().isEmpty()) {
                            lastLag = safeComputeTotalLag();
                            lastLagSampleNs = nowNs;
                        }
                    }

                    logWindow(windowStartNs, windowCount, totalCount, lastLag);

                    // Reset window
                    windowStartNs = nowNs;
                    windowCount = 0L;
                    lastReportNs = nowNs;
                }
            }
        } finally {
            try {
                // crucial for immediate rebalance on exit
                consumer.close();
            } catch (Exception ignored) {
            }
            log.info("StreamKernelConsumer stopped. Total records consumed={}", totalCount);
        }
    }

    private void logWindow(long windowStartNs, long windowCount, long totalCount, long totalLag) {
        final long nowNs = System.nanoTime();
        final long elapsedNs = nowNs - windowStartNs;

        if (elapsedNs <= 0) return;

        final double seconds = elapsedNs / 1_000_000_000.0;
        final double eps = windowCount / seconds;

        if (totalLag >= 0) {
            log.info("Consumed {} records in ~{}s → ~{} EPS (total={}, lag={})",
                    windowCount,
                    String.format("%.3f", seconds),
                    String.format("%,.0f", eps),
                    totalCount,
                    totalLag
            );
        } else {
            log.info("Consumed {} records in ~{}s → ~{} EPS (total={})",
                    windowCount,
                    String.format("%.3f", seconds),
                    String.format("%,.0f", eps),
                    totalCount
            );
        }
    }

    private long safeComputeTotalLag() {
        try {
            final Set<TopicPartition> assignment = consumer.assignment();
            if (assignment == null || assignment.isEmpty()) return -1L;

            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);

            long totalLag = 0L;
            for (TopicPartition tp : assignment) {
                final long end = endOffsets.getOrDefault(tp, 0L);
                final long pos = consumer.position(tp);
                if (end > pos) totalLag += (end - pos);
            }
            return totalLag;
        } catch (Exception e) {
            // Do not fail the consumer loop due to lag sampling issues
            log.debug("Lag sampling failed: {}", e.getMessage());
            return -1L;
        }
    }

    private void shutdown() {
        if (!running.compareAndSet(true, false)) return;
        log.info("Shutdown requested for StreamKernelConsumer...");
        try {
            consumer.wakeup(); // interrupts poll() promptly
        } catch (Exception ignored) {
        }
    }

    // ---------------------------------------------------------
    // MAIN ENTRYPOINT
    // ---------------------------------------------------------
    public static void main(String[] args) {
        final String topic =
                (args.length > 0 && args[0] != null && !args[0].isBlank())
                        ? args[0].trim()
                        : getString(P_TOPIC_DEFAULT, DEFAULT_TOPIC);

        new StreamKernelConsumer(topic).run();
    }

    // ---- system property helpers ----

    private static String getString(String key, String def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        return v.isEmpty() ? def : v;
    }

    private static long getLong(String key, long def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        if (v.isEmpty()) return def;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }
}
