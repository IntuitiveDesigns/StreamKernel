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

package com.example.streamkernel.kafka.output;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class KafkaSink implements OutputSink<String>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean syncSend;
    private final String pipelineName;

    // Local counters
    private final LongAdder sentOk = new LongAdder();
    private final LongAdder sentFail = new LongAdder();
    private final LongAdder inFlight = new LongAdder();

    // Backpressure (acked-based)
    private final long maxInFlightRecords;
    private final long inFlightWaitMs;

    // Metrics (Micrometer)
    private final Counter okCounter;
    private final Counter failCounter;
    private final Timer sendLatencyTimer;

    // Rate-limited logging
    private final long errorLogIntervalMs;
    private final AtomicLong lastErrorLogMs = new AtomicLong(0);
    private final LongAdder suppressedErrorLogs = new LongAdder();

    public KafkaSink(
            String topic,
            Properties props,
            boolean syncSend,
            MetricsRuntime metrics,
            String pipelineName
    ) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.syncSend = syncSend;
        this.pipelineName = (pipelineName == null || pipelineName.isBlank()) ? "unknown" : pipelineName;

        this.maxInFlightRecords = parseLong(props.getProperty("kafka.inflight.max", "500000"), 500000L);
        this.inFlightWaitMs = Math.max(0L, parseLong(props.getProperty("kafka.inflight.wait.ms", "1"), 1L));
        this.errorLogIntervalMs = Math.max(100L, parseLong(props.getProperty("streamkernel.sink.error.log.interval.ms", "1000"), 1000L));

        this.producer = new KafkaProducer<>(Objects.requireNonNull(props, "props"));

        MeterRegistry registry = (metrics != null && metrics.enabled()) ? metrics.registry() : null;
        if (registry != null) {
            this.okCounter = registry.counter("streamkernel_kafka_send_ok_total", "pipeline", this.pipelineName, "topic", this.topic);
            this.failCounter = registry.counter("streamkernel_kafka_send_fail_total", "pipeline", this.pipelineName, "topic", this.topic);
            this.sendLatencyTimer = registry.timer("streamkernel_kafka_send_latency", "pipeline", this.pipelineName, "topic", this.topic);
        } else {
            this.okCounter = null;
            this.failCounter = null;
            this.sendLatencyTimer = null;
        }

        log.info("KafkaSink created. topic='{}' syncSend={} pipeline='{}'", this.topic, this.syncSend, this.pipelineName);
    }

    public static KafkaSink fromConfig(PipelineConfig config, String topic, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        String pipelineName = config.getProperty("pipeline.name", "StreamKernel");
        boolean syncSend = Boolean.parseBoolean(config.getProperty("kafka.producer.sync", "false"));

        Properties props = buildProducerProps(config);

        // Non-Kafka properties
        props.putIfAbsent("streamkernel.sink.error.log.interval.ms", config.getProperty("streamkernel.sink.error.log.interval.ms", "1000"));
        props.putIfAbsent("kafka.inflight.max", config.getProperty("kafka.inflight.max", "500000"));
        props.putIfAbsent("kafka.inflight.wait.ms", config.getProperty("kafka.inflight.wait.ms", "1"));

        return new KafkaSink(topic, props, syncSend, metrics, pipelineName);
    }

    private static Properties buildProducerProps(PipelineConfig config) {
        Properties props = new Properties();

        // Standard Configs
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.broker", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getProperty("kafka.producer.client.id", config.getProperty("pipeline.name", "StreamKernel")));
        props.put(ProducerConfig.ACKS_CONFIG, config.getProperty("kafka.producer.acks", "1"));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getProperty("kafka.producer.compression", "lz4"));

        // Tuning
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(config.getProperty("kafka.producer.batch.size", "16384")));
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(config.getProperty("kafka.producer.linger.ms", "0")));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.parseLong(config.getProperty("kafka.producer.buffer.memory", "33554432")));

        // --- SECURITY PASSTHROUGH (mTLS / SASL) ---
        // Dynamically copy any property starting with kafka.ssl.*, kafka.security.*, or kafka.sasl.*
        for (String key : config.keys()) {
            if (key.startsWith("kafka.ssl.") || key.startsWith("kafka.security.") || key.startsWith("kafka.sasl.")) {
                // Remove the "kafka." prefix to match standard Kafka Client properties
                // e.g., "kafka.ssl.keystore.location" -> "ssl.keystore.location"
                String realKey = key.substring(6);
                props.put(realKey, config.getProperty(key));
            }
        }

        return props;
    }

    @Override
    public void write(PipelinePayload<String> payload) throws Exception {
        applyBackpressureIfNeeded();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload.id(), payload.data());

        if (syncSend) {
            long start = System.nanoTime();
            inFlight.increment();
            try {
                producer.send(record).get();
                markOk(start);
            } catch (Exception e) {
                markFail(start, payload.id(), e);
                throw e;
            } finally {
                inFlight.decrement();
            }
        } else {
            final long start = System.nanoTime();
            inFlight.increment();
            producer.send(record, (metadata, exception) -> {
                try {
                    if (exception == null) markOk(start);
                    else markFail(start, payload.id(), exception);
                } finally {
                    inFlight.decrement();
                }
            });
        }
    }

    private void applyBackpressureIfNeeded() {
        if (maxInFlightRecords <= 0) return;
        if (inFlight.sum() <= maxInFlightRecords) return;
        while (inFlight.sum() > maxInFlightRecords) {
            if (inFlightWaitMs <= 0) Thread.yield();
            else try { Thread.sleep(inFlightWaitMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
    }

    private void markOk(long startNanos) {
        sentOk.increment();
        if (okCounter != null) okCounter.increment();
        if (sendLatencyTimer != null) sendLatencyTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    private void markFail(long startNanos, String key, Throwable exception) {
        sentFail.increment();
        if (failCounter != null) failCounter.increment();
        if (sendLatencyTimer != null) sendLatencyTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);

        long now = System.currentTimeMillis();
        long last = lastErrorLogMs.get();
        if (now - last >= errorLogIntervalMs && lastErrorLogMs.compareAndSet(last, now)) {
            long suppressed = suppressedErrorLogs.sumThenReset();
            log.error("Kafka send failed. suppressed={} topic={} ex={}", suppressed, topic, exception.getMessage());
        } else {
            suppressedErrorLogs.increment();
        }
    }

    private static long parseLong(String value, long fallback) {
        try { return Long.parseLong(value.trim()); } catch (Exception e) { return fallback; }
    }

    public long sentOkTotal() { return sentOk.sum(); }
    public long sentFailTotal() { return sentFail.sum(); }
    public long inFlightTotal() { return inFlight.sum(); }

    @Override
    public void close() {
        try { producer.close(Duration.ofSeconds(5)); }
        catch (Exception e) { log.warn("Producer close failed", e); }
    }
}