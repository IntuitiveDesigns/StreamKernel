/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.spi.DlqSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class KafkaDlqBytesSink<I> implements OutputSink<I>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqBytesSink.class);

    // Internal config keys
    private static final String CFG_BROKER = "kafka.broker";
    private static final String CFG_PIPELINE_NAME = "pipeline.name";
    private static final String CFG_DLQ_TOPIC = "kafka.topic.dlq";

    private static final String CFG_DLQ_CLIENT_ID = "dlq.kafka.producer.client.id";
    private static final String CFG_DLQ_ACKS = "dlq.kafka.producer.acks";
    private static final String CFG_DLQ_ENABLE_IDEMPOTENCE = "dlq.kafka.producer.idempotence";
    private static final String CFG_DLQ_RETRIES = "dlq.kafka.producer.retries";
    private static final String CFG_DLQ_DELIVERY_TIMEOUT_MS = "dlq.kafka.producer.delivery.timeout.ms";
    private static final String CFG_DLQ_REQUEST_TIMEOUT_MS = "dlq.kafka.producer.request.timeout.ms";
    private static final String CFG_DLQ_MAX_IN_FLIGHT = "dlq.kafka.producer.max.in.flight.requests.per.connection";

    private static final String CFG_DLQ_COMPRESSION = "dlq.kafka.producer.compression";
    private static final String CFG_DLQ_BATCH_SIZE = "dlq.kafka.producer.batch.size";
    private static final String CFG_DLQ_LINGER_MS = "dlq.kafka.producer.linger.ms";
    private static final String CFG_DLQ_BUFFER_MEMORY = "dlq.kafka.producer.buffer.memory";

    private static final String CFG_DLQ_ERROR_LOG_INTERVAL_MS = "streamkernel.dlq.error.log.interval.ms";
    private static final String CFG_DLQ_CLOSE_TIMEOUT_MS = "streamkernel.dlq.close.timeout.ms";

    // Defaults (favor safety; still performant)
    private static final String DEFAULT_BROKER = "localhost:9092";
    private static final String DEFAULT_PIPELINE_NAME = "StreamKernel";
    private static final String DEFAULT_ACKS = "all";
    private static final boolean DEFAULT_IDEMPOTENCE = true;
    private static final int DEFAULT_RETRIES = Integer.MAX_VALUE;
    private static final int DEFAULT_DELIVERY_TIMEOUT_MS = 120_000;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_MAX_IN_FLIGHT = 5;

    private static final String DEFAULT_COMPRESSION = "lz4";
    private static final int DEFAULT_BATCH_SIZE = 65_536;
    private static final int DEFAULT_LINGER_MS = 5;
    private static final long DEFAULT_BUFFER_MEMORY = 33_554_432L; // 32MiB

    private static final long DEFAULT_ERROR_LOG_INTERVAL_MS = 1_000L;
    private static final long DEFAULT_CLOSE_TIMEOUT_MS = 5_000L;

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;
    private final DlqSerializer<I> serializer;

    private final long errorLogIntervalMs;
    private final AtomicLong lastErrorLogMs = new AtomicLong(0L);
    private final LongAdder suppressedErrors = new LongAdder();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long closeTimeoutMs;

    public KafkaDlqBytesSink(String topic, Properties props, DlqSerializer<I> serializer) {
        this.topic = requireNonBlank(topic, "topic");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.producer = new KafkaProducer<>(Objects.requireNonNull(props, "props"));

        this.errorLogIntervalMs = Math.max(100L, getLong(props, CFG_DLQ_ERROR_LOG_INTERVAL_MS, DEFAULT_ERROR_LOG_INTERVAL_MS));
        this.closeTimeoutMs = Math.max(1_000L, getLong(props, CFG_DLQ_CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS));

        log.info("DLQ Sink Active: topic='{}' serializer='{}'", this.topic, serializer.getClass().getSimpleName());
    }

    public static <I> KafkaDlqBytesSink<I> fromConfig(PipelineConfig config, DlqSerializer<I> serializer) {
        Objects.requireNonNull(config, "config");
        final String topic = config.getString(CFG_DLQ_TOPIC, null);
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration key: " + CFG_DLQ_TOPIC);
        }
        return new KafkaDlqBytesSink<>(topic, buildProducerProps(config), serializer);
    }

    public static Properties buildProducerProps(PipelineConfig config) {
        Objects.requireNonNull(config, "config");
        final Properties props = new Properties();

        // 1) Connection
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(CFG_BROKER, DEFAULT_BROKER));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        final String pipelineName = config.getString(CFG_PIPELINE_NAME, DEFAULT_PIPELINE_NAME);
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                config.getString(CFG_DLQ_CLIENT_ID, pipelineName + "-DLQ"));

        // 2) Durability / Safety
        props.put(ProducerConfig.ACKS_CONFIG, config.getString(CFG_DLQ_ACKS, DEFAULT_ACKS));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, getBoolean(config, CFG_DLQ_ENABLE_IDEMPOTENCE, DEFAULT_IDEMPOTENCE));
        props.put(ProducerConfig.RETRIES_CONFIG, getInt(config, CFG_DLQ_RETRIES, DEFAULT_RETRIES));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, getInt(config, CFG_DLQ_DELIVERY_TIMEOUT_MS, DEFAULT_DELIVERY_TIMEOUT_MS));
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, getInt(config, CFG_DLQ_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS));

        // When idempotence is enabled, Kafka defaults max.in.flight <= 5 for safety depending on version,
        // but we set it explicitly to avoid surprises across client upgrades.
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, getInt(config, CFG_DLQ_MAX_IN_FLIGHT, DEFAULT_MAX_IN_FLIGHT));

        // 3) Throughput tuning (DLQ is typically low-volume; these are safe defaults)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString(CFG_DLQ_COMPRESSION, DEFAULT_COMPRESSION));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, getInt(config, CFG_DLQ_BATCH_SIZE, DEFAULT_BATCH_SIZE));
        props.put(ProducerConfig.LINGER_MS_CONFIG, getInt(config, CFG_DLQ_LINGER_MS, DEFAULT_LINGER_MS));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, getLong(config, CFG_DLQ_BUFFER_MEMORY, DEFAULT_BUFFER_MEMORY));

        // 4) Security propagation (copy known Kafka client keys)
        // Prefer explicit allow-list rather than loose prefixes to reduce accidental propagation of unrelated keys.
        copyIfPresent(config, props, "kafka.security.protocol", CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);

        // SSL (common)
        copyIfPresent(config, props, "kafka.ssl.truststore.location", SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        copyIfPresent(config, props, "kafka.ssl.truststore.password", SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        copyIfPresent(config, props, "kafka.ssl.truststore.type", SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);

        copyIfPresent(config, props, "kafka.ssl.keystore.location", SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        copyIfPresent(config, props, "kafka.ssl.keystore.password", SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        copyIfPresent(config, props, "kafka.ssl.keystore.type", SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        copyIfPresent(config, props, "kafka.ssl.key.password", SslConfigs.SSL_KEY_PASSWORD_CONFIG);

        copyIfPresent(config, props, "kafka.ssl.endpoint.identification.algorithm", SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);

        // SASL (common)
        copyIfPresent(config, props, "kafka.sasl.mechanism", "sasl.mechanism");
        copyIfPresent(config, props, "kafka.sasl.jaas.config", "sasl.jaas.config");
        copyIfPresent(config, props, "kafka.sasl.client.callback.handler.class", "sasl.client.callback.handler.class");
        copyIfPresent(config, props, "kafka.sasl.login.callback.handler.class", "sasl.login.callback.handler.class");
        copyIfPresent(config, props, "kafka.sasl.login.class", "sasl.login.class");

        // 5) Internal settings
        props.put(CFG_DLQ_ERROR_LOG_INTERVAL_MS, config.getString(CFG_DLQ_ERROR_LOG_INTERVAL_MS, Long.toString(DEFAULT_ERROR_LOG_INTERVAL_MS)));
        props.put(CFG_DLQ_CLOSE_TIMEOUT_MS, config.getString(CFG_DLQ_CLOSE_TIMEOUT_MS, Long.toString(DEFAULT_CLOSE_TIMEOUT_MS)));

        return props;
    }

    @Override
    public void write(PipelinePayload<I> payload) {
        if (payload == null) return;
        if (closed.get()) return;

        final byte[] key;
        final byte[] val;

        try {
            key = serializer.key(payload);
            val = serializer.value(payload);
        } catch (Exception e) {
            logError("DLQ serialization failed", e);
            return;
        }

        try {
            final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, val);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logError("DLQ async send failed", exception);
                }
            });
        } catch (Exception e) {
            logError("DLQ send invocation failed", e);
        }
    }

    /**
     * Rate-limited error logger to prevent log flooding during outages.
     */
    private void logError(String context, Throwable ex) {
        final long now = System.currentTimeMillis();
        final long last = lastErrorLogMs.get();

        if (now - last >= errorLogIntervalMs) {
            if (lastErrorLogMs.compareAndSet(last, now)) {
                final long sup = suppressedErrors.sumThenReset();
                if (sup > 0) {
                    log.error("{} (suppressed {} similar errors): {}", context, sup, safeMessage(ex));
                } else {
                    log.error("{}: {}", context, safeMessage(ex));
                }
            } else {
                suppressedErrors.increment();
            }
        } else {
            suppressedErrors.increment();
        }
    }

    private static String safeMessage(Throwable t) {
        if (t == null) return "unknown";
        final String m = t.getMessage();
        return (m == null || m.isBlank()) ? t.getClass().getSimpleName() : m;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        log.info("Closing DLQ Sink (topic={})...", topic);

        try {
            // Flush is safer than relying on close timeout alone; close also flushes, but explicit flush makes intent clear.
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

    // --- Helpers ---

    private static void copyIfPresent(PipelineConfig cfg, Properties dst, String cfgKey, String kafkaKey) {
        final String v = cfg.getString(cfgKey, null);
        if (v != null && !v.isBlank()) {
            dst.put(kafkaKey, v);
        }
    }

    private static int getInt(PipelineConfig cfg, String key, int def) {
        try { return cfg.getInt(key, def); }
        catch (Exception ignored) {
            try {
                return Integer.parseInt(cfg.getString(key, Integer.toString(def)).trim());
            } catch (Exception ignored2) {
                return def;
            }
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

    private static long getLong(Properties p, String key, long def) {
        try {
            final String s = p.getProperty(key);
            if (s == null) return def;
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

    private static String requireNonBlank(String s, String name) {
        Objects.requireNonNull(name, "name");
        if (s == null) throw new IllegalArgumentException(name + " is required");
        final String t = s.trim();
        if (t.isEmpty()) throw new IllegalArgumentException(name + " is required");
        return t;
    }
}
