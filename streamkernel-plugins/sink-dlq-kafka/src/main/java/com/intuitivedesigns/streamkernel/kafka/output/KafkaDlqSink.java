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

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Durable Kafka DLQ sink (generic input type I).
 *
 * Key: payload.id()
 * Value: String.valueOf(payload.data())
 *
 * Note: If you want DLQ to preserve original bytes (Avro/Protobuf), prefer KafkaDlqBytesSink + DlqSerializer.
 */
public final class KafkaDlqSink<I> implements OutputSink<I>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqSink.class);

    // Config keys
    private static final String CFG_BROKER = "kafka.broker";
    private static final String CFG_PIPELINE_NAME = "pipeline.name";

    private static final String CFG_DLQ_TOPIC = "kafka.topic.dlq";
    private static final String CFG_DLQ_TOPIC_LEGACY = "dlq.topic";

    private static final String CFG_DLQ_CLIENT_ID = "dlq.kafka.producer.client.id";
    private static final String CFG_DLQ_ACKS = "dlq.kafka.producer.acks";
    private static final String CFG_DLQ_IDEMPOTENCE = "dlq.kafka.producer.idempotence";
    private static final String CFG_DLQ_RETRIES = "dlq.kafka.producer.retries";
    private static final String CFG_DLQ_DELIVERY_TIMEOUT_MS = "dlq.kafka.producer.delivery.timeout.ms";
    private static final String CFG_DLQ_REQUEST_TIMEOUT_MS = "dlq.kafka.producer.request.timeout.ms";
    private static final String CFG_DLQ_MAX_IN_FLIGHT = "dlq.kafka.producer.max.in.flight";

    private static final String CFG_DLQ_COMPRESSION = "dlq.kafka.producer.compression";
    private static final String CFG_DLQ_BATCH_SIZE = "dlq.kafka.producer.batch.size";
    private static final String CFG_DLQ_LINGER_MS = "dlq.kafka.producer.linger.ms";
    private static final String CFG_DLQ_BUFFER_MEMORY = "dlq.kafka.producer.buffer.memory";

    private static final String CFG_ERROR_LOG_INTERVAL_MS = "streamkernel.dlq.error.log.interval.ms";
    private static final String CFG_CLOSE_TIMEOUT_MS = "streamkernel.dlq.close.timeout.ms";

    // Defaults (safety-first)
    private static final String DEFAULT_BROKER = "localhost:9092";
    private static final String DEFAULT_PIPELINE_NAME = "StreamKernel";
    private static final String DEFAULT_DLQ_TOPIC = "streamkernel-dlq";

    private static final String DEFAULT_ACKS = "all";
    private static final boolean DEFAULT_IDEMPOTENCE = true;
    private static final int DEFAULT_RETRIES = Integer.MAX_VALUE;
    private static final int DEFAULT_DELIVERY_TIMEOUT_MS = 120_000;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_MAX_IN_FLIGHT = 5;

    private static final String DEFAULT_COMPRESSION = "lz4";
    private static final int DEFAULT_BATCH_SIZE = 65_536;
    private static final int DEFAULT_LINGER_MS = 5;
    private static final long DEFAULT_BUFFER_MEMORY = 33_554_432L;

    private static final long DEFAULT_ERROR_LOG_INTERVAL_MS = 1_000L;
    private static final long DEFAULT_CLOSE_TIMEOUT_MS = 5_000L;

    private final KafkaProducer<String, String> producer;
    private final String topic;

    private final long errorLogIntervalMs;
    private final AtomicLong lastErrorLogMs = new AtomicLong(0L);
    private final LongAdder suppressedErrors = new LongAdder();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long closeTimeoutMs;

    public KafkaDlqSink(String topic, Properties props) {
        this.topic = requireNonBlank(topic, "topic");
        this.producer = new KafkaProducer<>(Objects.requireNonNull(props, "props"));

        this.errorLogIntervalMs = Math.max(100L, getLong(props, CFG_ERROR_LOG_INTERVAL_MS, DEFAULT_ERROR_LOG_INTERVAL_MS));
        this.closeTimeoutMs = Math.max(1_000L, getLong(props, CFG_CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS));

        log.info("KafkaDlqSink active. topic='{}'", this.topic);
    }

    public static <I> KafkaDlqSink<I> fromConfig(PipelineConfig config) {
        Objects.requireNonNull(config, "config");

        final String topic = firstNonBlank(
                config.getString(CFG_DLQ_TOPIC, null),
                config.getString(CFG_DLQ_TOPIC_LEGACY, null),
                DEFAULT_DLQ_TOPIC
        );

        final Properties props = buildProducerProps(config);

        // Internal tuning knobs (kept in props so constructor stays small)
        props.putIfAbsent(CFG_ERROR_LOG_INTERVAL_MS, config.getString(CFG_ERROR_LOG_INTERVAL_MS, Long.toString(DEFAULT_ERROR_LOG_INTERVAL_MS)));
        props.putIfAbsent(CFG_CLOSE_TIMEOUT_MS, config.getString(CFG_CLOSE_TIMEOUT_MS, Long.toString(DEFAULT_CLOSE_TIMEOUT_MS)));

        return new KafkaDlqSink<>(topic, props);
    }

    private static Properties buildProducerProps(PipelineConfig config) {
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(CFG_BROKER, DEFAULT_BROKER));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final String pipelineName = config.getString(CFG_PIPELINE_NAME, DEFAULT_PIPELINE_NAME);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getString(CFG_DLQ_CLIENT_ID, pipelineName + "-DLQ"));

        // Durable-by-default DLQ semantics
        props.put(ProducerConfig.ACKS_CONFIG, config.getString(CFG_DLQ_ACKS, DEFAULT_ACKS));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, getBoolean(config, CFG_DLQ_IDEMPOTENCE, DEFAULT_IDEMPOTENCE));
        props.put(ProducerConfig.RETRIES_CONFIG, getInt(config, CFG_DLQ_RETRIES, DEFAULT_RETRIES));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, getInt(config, CFG_DLQ_DELIVERY_TIMEOUT_MS, DEFAULT_DELIVERY_TIMEOUT_MS));
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, getInt(config, CFG_DLQ_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, getInt(config, CFG_DLQ_MAX_IN_FLIGHT, DEFAULT_MAX_IN_FLIGHT));

        // Throughput knobs (safe defaults)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString(CFG_DLQ_COMPRESSION, DEFAULT_COMPRESSION));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, getInt(config, CFG_DLQ_BATCH_SIZE, DEFAULT_BATCH_SIZE));
        props.put(ProducerConfig.LINGER_MS_CONFIG, getInt(config, CFG_DLQ_LINGER_MS, DEFAULT_LINGER_MS));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, getLong(config, CFG_DLQ_BUFFER_MEMORY, DEFAULT_BUFFER_MEMORY));

        // SECURITY PASSTHROUGH (same behavior as KafkaSink), but defensive against nulls
        for (String key : config.keys()) {
            if (key == null) continue;
            if (key.startsWith("kafka.ssl.") || key.startsWith("kafka.security.") || key.startsWith("kafka.sasl.")) {
                final String v = config.getString(key, null);
                if (v == null || v.isBlank()) continue;

                final String realKey = key.substring(6); // remove "kafka."
                props.put(realKey, v);
            }
        }

        return props;
    }

    @Override
    public void write(PipelinePayload<I> payload) {
        if (payload == null) return;
        if (closed.get()) return;

        final String key = payload.id();
        final String value = String.valueOf(payload.data());

        try {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) return;

                final long now = System.currentTimeMillis();
                final long last = lastErrorLogMs.get();
                if (now - last >= errorLogIntervalMs && lastErrorLogMs.compareAndSet(last, now)) {
                    final long suppressed = suppressedErrors.sumThenReset();
                    log.error("DLQ Kafka send failed. suppressed={} topic={} ex={}", suppressed, topic, safeMessage(exception));
                } else {
                    suppressedErrors.increment();
                }
            });
        } catch (Exception e) {
            rateLimitedLog("DLQ Kafka send invocation failed", e);
        }
    }

    private void rateLimitedLog(String context, Throwable ex) {
        final long now = System.currentTimeMillis();
        final long last = lastErrorLogMs.get();

        if (now - last >= errorLogIntervalMs && lastErrorLogMs.compareAndSet(last, now)) {
            final long suppressed = suppressedErrors.sumThenReset();
            log.error("{} (suppressed {} similar errors): {}", context, suppressed, safeMessage(ex));
        } else {
            suppressedErrors.increment();
        }
    }

    private static String safeMessage(Throwable t) {
        if (t == null) return "unknown";
        final String m = t.getMessage();
        return (m == null || m.isBlank()) ? t.getClass().getSimpleName() : m;
    }

    private static long getLong(Properties p, String key, long def) {
        try {
            final String v = p.getProperty(key);
            if (v == null) return def;
            return Long.parseLong(v.trim());
        } catch (Exception ignored) {
            return def;
        }
    }

    private static int getInt(PipelineConfig cfg, String key, int def) {
        try { return cfg.getInt(key, def); }
        catch (Exception ignored) {
            try { return Integer.parseInt(cfg.getString(key, Integer.toString(def)).trim()); }
            catch (Exception ignored2) { return def; }
        }
    }

    private static long getLong(PipelineConfig cfg, String key, long def) {
        try {
            final String s = cfg.getString(key, Long.toString(def));
            return Long.parseLong(s.trim());
        } catch (Exception ignored) {
            return def;
        }
    }

    private static boolean getBoolean(PipelineConfig cfg, String key, boolean def) {
        try {
            final String s = cfg.getString(key, Boolean.toString(def));
            return Boolean.parseBoolean(s.trim());
        } catch (Exception ignored) {
            return def;
        }
    }

    private static String firstNonBlank(String a, String b, String c) {
        final String na = normalize(a);
        if (na != null) return na;
        final String nb = normalize(b);
        if (nb != null) return nb;
        final String nc = normalize(c);
        return (nc != null) ? nc : DEFAULT_DLQ_TOPIC;
    }

    private static String normalize(String s) {
        if (s == null) return null;
        final String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String requireNonBlank(String s, String name) {
        Objects.requireNonNull(name, "name");
        if (s == null) throw new IllegalArgumentException(name + " is required");
        final String t = s.trim();
        if (t.isEmpty()) throw new IllegalArgumentException(name + " is required");
        return t;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        try {
            producer.flush();
        } catch (Exception e) {
            log.warn("DLQ producer flush failed", e);
        }

        try {
            producer.close(Duration.ofMillis(closeTimeoutMs));
        } catch (Exception e) {
            log.warn("DLQ producer close failed", e);
        }
    }
}
