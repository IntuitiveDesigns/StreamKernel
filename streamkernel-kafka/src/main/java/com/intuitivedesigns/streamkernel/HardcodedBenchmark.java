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

package com.intuitivedesigns.streamkernel;

import com.intuitivedesigns.streamkernel.bench.SyntheticSource;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Configurable benchmark that keeps the "hardcoded" defaults but allows overrides via -D system properties.
 *
 * Example:
 *   java -Dbench.bootstrap=localhost:9092 -Dbench.topic=streamkernel-bench-test -Dbench.threads=50 -Dbench.batch=4000 ...
 */
public final class HardcodedBenchmark {

    // ---- System property keys ----
    private static final String P_BOOTSTRAP = "bench.bootstrap";
    private static final String P_TOPIC = "bench.topic";
    private static final String P_ACKS = "bench.acks";
    private static final String P_COMPRESSION = "bench.compression";
    private static final String P_LINGER_MS = "bench.linger.ms";
    private static final String P_BATCH_BYTES = "bench.batch.bytes";
    private static final String P_BUFFER_BYTES = "bench.buffer.bytes";
    private static final String P_MAX_REQUEST_BYTES = "bench.max.request.bytes";

    private static final String P_PAYLOAD_BYTES = "bench.payload.bytes";
    private static final String P_HIGH_ENTROPY = "bench.high.entropy";

    private static final String P_THREADS = "bench.threads";
    private static final String P_APP_BATCH = "bench.batch";
    private static final String P_REPORT_EVERY_MS = "bench.report.ms";
    private static final String P_FLUSH_ON_STOP = "bench.flush.on.stop";

    // ---- Defaults (match your original) ----
    private static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    private static final String DEFAULT_TOPIC = "streamkernel-bench-test";
    private static final String DEFAULT_ACKS = "all";
    private static final String DEFAULT_COMPRESSION = "lz4";
    private static final int DEFAULT_LINGER_MS = 50;
    private static final int DEFAULT_BATCH_BYTES = 4 * 1024 * 1024;           // 4MB
    private static final long DEFAULT_BUFFER_BYTES = 256L * 1024L * 1024L;    // 256MB
    private static final int DEFAULT_MAX_REQUEST_BYTES = 10 * 1024 * 1024;    // 10MB

    private static final int DEFAULT_PAYLOAD_BYTES = 1024;
    private static final boolean DEFAULT_HIGH_ENTROPY = false;

    private static final int DEFAULT_THREADS = 50;
    private static final int DEFAULT_APP_BATCH = 4000;
    private static final int DEFAULT_REPORT_MS = 1000;
    private static final boolean DEFAULT_FLUSH_ON_STOP = true;

    private HardcodedBenchmark() {}

    public static void main(String[] args) {
        System.out.println("=== BENCHMARK (Configurable Defaults) ===");

        final String bootstrap = getString(P_BOOTSTRAP, DEFAULT_BOOTSTRAP);
        final String topic = getString(P_TOPIC, DEFAULT_TOPIC);

        final String acks = getString(P_ACKS, DEFAULT_ACKS);
        final String compression = getString(P_COMPRESSION, DEFAULT_COMPRESSION);

        final int lingerMs = clampInt(getInt(P_LINGER_MS, DEFAULT_LINGER_MS), 0, 60_000);
        final int batchBytes = clampInt(getInt(P_BATCH_BYTES, DEFAULT_BATCH_BYTES), 0, Integer.MAX_VALUE);
        final long bufferBytes = clampLong(getLong(P_BUFFER_BYTES, DEFAULT_BUFFER_BYTES), 0, Long.MAX_VALUE);
        final int maxRequestBytes = clampInt(getInt(P_MAX_REQUEST_BYTES, DEFAULT_MAX_REQUEST_BYTES), 0, Integer.MAX_VALUE);

        final int payloadBytes = clampInt(getInt(P_PAYLOAD_BYTES, DEFAULT_PAYLOAD_BYTES), 0, Integer.MAX_VALUE);
        final boolean highEntropy = getBoolean(P_HIGH_ENTROPY, DEFAULT_HIGH_ENTROPY);

        final int threads = clampInt(getInt(P_THREADS, DEFAULT_THREADS), 1, Integer.MAX_VALUE);
        final int appBatchSize = clampInt(getInt(P_APP_BATCH, DEFAULT_APP_BATCH), 1, Integer.MAX_VALUE);
        final int reportEveryMs = clampInt(getInt(P_REPORT_EVERY_MS, DEFAULT_REPORT_MS), 250, 60_000);
        final boolean flushOnStop = getBoolean(P_FLUSH_ON_STOP, DEFAULT_FLUSH_ON_STOP);

        System.out.printf(
                "CONFIG: bootstrap=%s topic=%s threads=%d appBatch=%d payloadBytes=%d entropy=%s%n",
                bootstrap, topic, threads, appBatchSize, payloadBytes, highEntropy ? "HIGH" : "LOW"
        );

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Performance tuning (defaults preserved; configurable)
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchBytes);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferBytes);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestBytes);

        // Sensible producer defaults for high-throughput tests (safe; Kafka default is true)
        props.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch stopLatch = new CountDownLatch(1);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            final SourceConnector<String> source = new SyntheticSource(payloadBytes, highEntropy);

            final var exec = Executors.newVirtualThreadPerTaskExecutor();
            final Semaphore permits = new Semaphore(threads, false);
            final LongAdder counter = new LongAdder();
            final long startNs = System.nanoTime();

            final ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(new NamedDaemonThreadFactory("bench-reporter"));

            reporter.scheduleAtFixedRate(() -> {
                long nowNs = System.nanoTime();
                double seconds = (nowNs - startNs) / 1_000_000_000.0;
                double eps = seconds <= 0 ? 0.0 : counter.sum() / seconds;
                System.out.printf("SPEED: %,.0f Events/Sec | Total: %,d%n", eps, counter.sum());
            }, reportEveryMs, reportEveryMs, TimeUnit.MILLISECONDS);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!running.compareAndSet(true, false)) return;
                System.out.println("Shutdown signal received...");
                try {
                    reporter.shutdownNow();
                } finally {
                    try {
                        if (flushOnStop) producer.flush();
                    } catch (Exception ignored) {}
                    try {
                        exec.shutdownNow();
                    } catch (Exception ignored) {}
                    stopLatch.countDown();
                }
            }, "bench-shutdown"));

            System.out.println("🚀 Starting Traffic... (Ctrl+C to stop)");

            while (running.get()) {
                final var batch = source.fetchBatch(appBatchSize);
                if (batch.isEmpty()) {
                    Thread.onSpinWait();
                    continue;
                }

                // Acquire permit BEFORE submitting to bound outstanding tasks
                permits.acquireUninterruptibly();

                exec.submit(() -> {
                    try {
                        for (PipelinePayload<String> record : batch) {
                            producer.send(new ProducerRecord<>(topic, record.id(), record.data()));
                        }
                        counter.add(batch.size());
                    } finally {
                        permits.release();
                    }
                });
            }

            stopLatch.await();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- helpers ----

    private static String getString(String key, String def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        return v.isEmpty() ? def : v;
    }

    private static boolean getBoolean(String key, boolean def) {
        String v = System.getProperty(key);
        return v == null ? def : Boolean.parseBoolean(v);
    }

    private static int getInt(String key, int def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        if (v.isEmpty()) return def;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return def;
        }
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

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }

    private static final class NamedDaemonThreadFactory implements ThreadFactory {
        private final String name;

        private NamedDaemonThreadFactory(String name) {
            this.name = Objects.requireNonNull(name, "name");
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        }
    }
}
