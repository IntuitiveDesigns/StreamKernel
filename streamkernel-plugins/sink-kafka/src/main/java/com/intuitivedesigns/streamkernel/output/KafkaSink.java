/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * High-performance Kafka Sink.
 *
 * Features:
 * - Optimized batching (linger/batch.size)
 * - Native backpressure (semaphore-based)
 * - Rate-limited error logging
 * - Optional Micrometer counters/timer
 */
public final class KafkaSink implements OutputSink<String>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    // ---- Internal knobs (NOT Kafka client configs) ----
    private static final String PROP_ERROR_LOG_INTERVAL_MS = "streamkernel.sink.error.log.interval.ms";
    private static final String PROP_INFLIGHT_MAX = "streamkernel.sink.inflight.max";

    // ---- Defaults ----
    private static final long DEFAULT_ERROR_LOG_INTERVAL_MS = 1_000L;
    private static final int DEFAULT_INFLIGHT_MAX = 100_000;

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean syncSend;
    private final String pipelineName;

    // Backpressure
    private final boolean backpressureEnabled;
    private final int maxInFlight;
    private final Semaphore inflightSemaphore;
    private final LongAdder inflightNow = new LongAdder();

    // Fast counters (always on)
    private final LongAdder sentOk = new LongAdder();
    private final LongAdder sentFail = new LongAdder();

    // Micrometer (optional)
    private final Counter okCounter;
    private final Counter failCounter;
    private final Timer sendLatencyTimer;

    // Rate-limited error logging
    private final long errorLogIntervalMs;
    private final AtomicLong lastErrorLogMs = new AtomicLong(0);
    private final LongAdder suppressedErrorLogs = new LongAdder();

    public KafkaSink(String topic,
                     Properties props,
                     boolean syncSend,
                     MetricsRuntime metrics,
                     String pipelineName) {

        this.topic = Objects.requireNonNull(topic, "topic");
        this.syncSend = syncSend;
        this.pipelineName = normalize(pipelineName, "StreamKernel");

        this.errorLogIntervalMs = Math.max(
                100L,
                parseLong(props.getProperty(PROP_ERROR_LOG_INTERVAL_MS, Long.toString(DEFAULT_ERROR_LOG_INTERVAL_MS)),
                        DEFAULT_ERROR_LOG_INTERVAL_MS)
        );

        this.maxInFlight = clampInt(
                parseInt(props.getProperty(PROP_INFLIGHT_MAX, Integer.toString(DEFAULT_INFLIGHT_MAX)),
                        DEFAULT_INFLIGHT_MAX),
                0,
                Integer.MAX_VALUE
        );

        if (maxInFlight > 0) {
            this.inflightSemaphore = new Semaphore(maxInFlight, false);
            this.backpressureEnabled = true;
        } else {
            this.inflightSemaphore = null;
            this.backpressureEnabled = false;
        }

        this.producer = new KafkaProducer<>(Objects.requireNonNull(props, "props"));

        // Metrics wiring (optional)
        MeterRegistry registry = (metrics != null && metrics.enabled() && metrics.registry() instanceof MeterRegistry mr)
                ? mr
                : null;

        if (registry != null) {
            this.okCounter = registry.counter(
                    "streamkernel_kafka_send_ok_total",
                    "pipeline", this.pipelineName,
                    "topic", this.topic
            );
            this.failCounter = registry.counter(
                    "streamkernel_kafka_send_fail_total",
                    "pipeline", this.pipelineName,
                    "topic", this.topic
            );
            this.sendLatencyTimer = registry.timer(
                    "streamkernel_kafka_send_latency",
                    "pipeline", this.pipelineName,
                    "topic", this.topic
            );
        } else {
            this.okCounter = null;
            this.failCounter = null;
            this.sendLatencyTimer = null;
        }

        log.info("KafkaSink active. topic='{}' sync={} inflight_limit={}",
                this.topic, this.syncSend, backpressureEnabled ? maxInFlight : "unbounded");
    }

    public static KafkaSink fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        String topic = config.getString("kafka.output.topic", "output-topic");

        final String pipelineName = config.getString("pipeline.name", "StreamKernel");
        final boolean syncSend = Boolean.parseBoolean(config.getString("kafka.producer.sync", "false"));

        final Properties props = buildProducerProps(config);

        // Internal knobs
        props.put(PROP_ERROR_LOG_INTERVAL_MS, config.getString(PROP_ERROR_LOG_INTERVAL_MS, Long.toString(DEFAULT_ERROR_LOG_INTERVAL_MS)));
        props.put(PROP_INFLIGHT_MAX, config.getString(PROP_INFLIGHT_MAX, Integer.toString(DEFAULT_INFLIGHT_MAX)));

        return new KafkaSink(topic, props, syncSend, metrics, pipelineName);
    }

    private static Properties buildProducerProps(PipelineConfig config) {
        final Properties props = new Properties();

        // Connectivity
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers", "localhost:9092"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                config.getString("kafka.producer.client.id", config.getString("pipeline.name", "StreamKernel")));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Durability
        props.put(ProducerConfig.ACKS_CONFIG, config.getString("kafka.producer.acks", "1"));
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // Performance
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString("kafka.producer.compression", "lz4"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(getInt(config, "kafka.producer.batch.size", 65_536)));
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(getInt(config, "kafka.producer.linger.ms", 5)));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(getLong(config, "kafka.producer.buffer.memory", 33_554_432L)));

        // Security passthrough: kafka.ssl.*, kafka.security.*, kafka.sasl.* -> strip "kafka."
        copySecurityProps(config, props);

        return props;
    }

    @Override
    public void write(PipelinePayload<String> payload) throws Exception {
        if (payload == null) return;

        boolean permitAcquired = false;
        long startNs = 0L;

        try {
            // Acquire permit for in-flight backpressure
            if (backpressureEnabled) {
                inflightSemaphore.acquire(); // blocking is fine for virtual threads
                permitAcquired = true;
                inflightNow.increment();
            }

            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, payload.id(), payload.data());

            startNs = System.nanoTime();

            if (syncSend) {
                // Sync send (blocking)
                producer.send(record).get();
                markOk(startNs);
                // release handled in finally
            } else {
                // Async send
                final long capturedStart = startNs;
                producer.send(record, (metadata, exception) -> {
                    try {
                        if (exception == null) {
                            markOk(capturedStart);
                        } else {
                            markFail(capturedStart, exception);
                        }
                    } finally {
                        if (backpressureEnabled) {
                            inflightNow.decrement();
                            inflightSemaphore.release();
                        }
                    }
                });

                // For async, callback releases the permit; prevent double-release in finally
                permitAcquired = false;
            }

        } catch (Exception e) {
            // If failure happened after acquire and before async callback installation,
            // ensure we record fail + release once.
            if (startNs != 0L) {
                markFail(startNs, e);
            } else {
                // even if startNs wasn't set, still count a failure
                sentFail.increment();
                if (failCounter != null) failCounter.increment();
                logRateLimited("Kafka write failed (pre-send)", e);
            }
            throw e;

        } finally {
            if (permitAcquired && backpressureEnabled) {
                inflightNow.decrement();
                inflightSemaphore.release();
            }
        }
    }

    // ---- Critical Fix: Override flush() ----
    @Override
    public void flush() {
        // Without this, orchestrator calls to flush() do nothing, risking data loss on checkpoint.
        producer.flush();
    }

    // ---- Metrics & Logging ----

    private void markOk(long startNanos) {
        sentOk.increment();
        if (okCounter != null) okCounter.increment();
        if (sendLatencyTimer != null && startNanos != 0L) {
            sendLatencyTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
    }

    private void markFail(long startNanos, Throwable exception) {
        sentFail.increment();
        if (failCounter != null) failCounter.increment();
        if (sendLatencyTimer != null && startNanos != 0L) {
            sendLatencyTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
        logRateLimited("Kafka write failed topic=" + topic, exception);
    }

    private void logRateLimited(String context, Throwable ex) {
        long now = System.currentTimeMillis();
        long last = lastErrorLogMs.get();

        if (now - last >= errorLogIntervalMs && lastErrorLogMs.compareAndSet(last, now)) {
            long suppressed = suppressedErrorLogs.sumThenReset();
            if (suppressed > 0) {
                log.error("{} (suppressed {} similar errors): {}", context, suppressed, ex.getMessage());
            } else {
                log.error("{}: {}", context, ex.getMessage());
            }
        } else {
            suppressedErrorLogs.increment();
        }
    }

    // ---- Introspection ----

    public long sentOkTotal() { return sentOk.sum(); }
    public long sentFailTotal() { return sentFail.sum(); }

    /**
     * Approximate current inflight messages.
     * Renamed from 'inFlightCount' to 'inFlightTotal' to match KafkaApp expectations.
     */
    public long inFlightTotal() { return inflightNow.sum(); }

    /** Remaining permits (useful for debugging). */
    public int availablePermits() { return backpressureEnabled ? inflightSemaphore.availablePermits() : -1; }

    // ---- Lifecycle ----

    @Override
    public void close() {
        log.info("Closing KafkaSink (topic={})...", topic);
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("KafkaSink close failed", e);
        }
    }

    // ---- Helpers ----

    private static void copySecurityProps(PipelineConfig cfg, Properties dst) {
        for (String key : cfg.keys()) {
            if (key == null) continue;
            if (key.startsWith("kafka.ssl.") || key.startsWith("kafka.security.") || key.startsWith("kafka.sasl.")) {
                String v = cfg.getString(key, null);
                if (v == null) continue;
                dst.put(key.substring(6), v); // strip "kafka."
            }
        }
    }

    private static int getInt(PipelineConfig cfg, String key, int def) {
        try { return Integer.parseInt(cfg.getString(key, Integer.toString(def)).trim()); }
        catch (Exception e) { return def; }
    }

    private static long getLong(PipelineConfig cfg, String key, long def) {
        try { return Long.parseLong(cfg.getString(key, Long.toString(def)).trim()); }
        catch (Exception e) { return def; }
    }

    private static long parseLong(String val, long def) {
        try { return Long.parseLong(val.trim()); } catch (Exception e) { return def; }
    }

    private static int parseInt(String val, int def) {
        try { return Integer.parseInt(val.trim()); } catch (Exception e) { return def; }
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static String normalize(String s, String def) {
        return (s == null || s.isBlank()) ? def : s.trim();
    }
}