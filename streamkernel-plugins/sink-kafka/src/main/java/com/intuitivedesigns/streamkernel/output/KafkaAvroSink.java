/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

/**
 * High-Performance Kafka Sink for Avro.
 * Supports Schema Registry, Compression, and Dynamic Record Mapping.
 */
public final class KafkaAvroSink implements OutputSink<String>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaAvroSink.class);

    // ---- Config keys (sink-scoped) ----
    private static final String CFG_SCHEMA_PATH = "sink.avro.schema.path";
    private static final String CFG_SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static final String CFG_METRIC_WRITTEN = "sink.kafka.messages.written";
    private static final String CFG_METRIC_WRITE_ERRORS = "sink.kafka.write.errors";
    private static final String CFG_METRIC_SERIALIZE_ERRORS = "sink.kafka.serialize.errors";
    private static final String CFG_ERROR_LOG_INTERVAL_MS = "sink.kafka.error.log.interval.ms";

    // Kafka producer tuning (sink-scoped)
    private static final String CFG_PRODUCER_COMPRESSION = "sink.kafka.producer.compression";
    private static final String CFG_PRODUCER_BATCH_SIZE = "sink.kafka.producer.batch.size";
    private static final String CFG_PRODUCER_LINGER_MS = "sink.kafka.producer.linger.ms";
    private static final String CFG_PRODUCER_BUFFER_MEMORY = "sink.kafka.producer.buffer.memory";
    private static final String CFG_PRODUCER_DELIVERY_TIMEOUT_MS = "sink.kafka.producer.delivery.timeout.ms";
    private static final String CFG_PRODUCER_MAX_IN_FLIGHT = "sink.kafka.producer.max.in.flight";
    private static final String CFG_PRODUCER_CLIENT_ID = "sink.kafka.producer.client.id";

    // Kafka common
    private static final String CFG_KAFKA_BROKER = "kafka.broker";
    private static final String CFG_PIPELINE_NAME = "pipeline.name";

    // Defaults
    private static final String DEFAULT_SCHEMA_PATH = "schema.avsc";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final long DEFAULT_ERROR_LOG_INTERVAL_MS = 1000L;

    private static final String DEFAULT_COMPRESSION = "lz4";
    private static final int DEFAULT_BATCH_SIZE = 65_536;
    private static final int DEFAULT_LINGER_MS = 5;
    private static final long DEFAULT_BUFFER_MEMORY = 33_554_432L;
    private static final int DEFAULT_DELIVERY_TIMEOUT_MS = 120_000;
    private static final int DEFAULT_MAX_IN_FLIGHT = 5;

    // ---- State ----
    private final KafkaProducer<String, GenericRecord> producer;
    private final String topic;
    private final Schema schema;
    private final MetricsRuntime metrics;
    private final BiConsumer<PipelinePayload<String>, GenericRecord> mapper;

    // Rate-limited error logging
    private final long errorLogIntervalMs;
    private final AtomicLong lastErrorLogMs = new AtomicLong(0);
    private final LongAdder suppressedErrors = new LongAdder();

    private final String metricWritten;
    private final String metricWriteErrors;
    private final String metricSerializeErrors;

    private KafkaAvroSink(String topic,
                          Properties props,
                          Schema schema,
                          MetricsRuntime metrics,
                          BiConsumer<PipelinePayload<String>, GenericRecord> mapper,
                          long errorLogIntervalMs,
                          String metricWritten,
                          String metricWriteErrors,
                          String metricSerializeErrors) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.errorLogIntervalMs = Math.max(100L, errorLogIntervalMs);

        this.metricWritten = Objects.requireNonNull(metricWritten, "metricWritten");
        this.metricWriteErrors = Objects.requireNonNull(metricWriteErrors, "metricWriteErrors");
        this.metricSerializeErrors = Objects.requireNonNull(metricSerializeErrors, "metricSerializeErrors");

        if (!props.containsKey(CFG_SCHEMA_REGISTRY_URL)) {
            throw new IllegalArgumentException("KafkaAvroSink requires '" + CFG_SCHEMA_REGISTRY_URL + "'");
        }

        this.producer = new KafkaProducer<>(Objects.requireNonNull(props, "props"));
        log.info("KafkaAvroSink active. topic='{}' schema='{}'", this.topic, this.schema.getFullName());
    }

    public static KafkaAvroSink fromConfig(PipelineConfig config, String topic, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(metrics, "metrics");

        final String schemaPath = config.getString(CFG_SCHEMA_PATH, DEFAULT_SCHEMA_PATH);
        final Schema schema = loadSchema(schemaPath);

        final Properties props = buildProducerProps(config);

        final long logIntervalMs = parseLong(
                config.getString(CFG_ERROR_LOG_INTERVAL_MS, Long.toString(DEFAULT_ERROR_LOG_INTERVAL_MS)),
                DEFAULT_ERROR_LOG_INTERVAL_MS
        );

        final String metricWritten = config.getString(CFG_METRIC_WRITTEN, "sink.kafka.messages.written");
        final String metricWriteErrors = config.getString(CFG_METRIC_WRITE_ERRORS, "sink.kafka.errors");
        final String metricSerializeErrors = config.getString(CFG_METRIC_SERIALIZE_ERRORS, "sink.kafka.serialization.errors");

        // Default mapping: id->customerId, data->name, timestamp->timestamp (if present)
        final BiConsumer<PipelinePayload<String>, GenericRecord> defaultMapper = (payload, record) -> {
            if (payload == null || record == null) return;
            Schema s = record.getSchema();

            if (s.getField("customerId") != null) {
                record.put("customerId", payload.id());
            }
            if (s.getField("name") != null) {
                record.put("name", payload.data());
            }
            if (s.getField("timestamp") != null && payload.timestamp() != null) {
                record.put("timestamp", payload.timestamp().toEpochMilli());
            }
        };

        return new KafkaAvroSink(
                topic,
                props,
                schema,
                metrics,
                defaultMapper,
                logIntervalMs,
                metricWritten,
                metricWriteErrors,
                metricSerializeErrors
        );
    }

    private static Properties buildProducerProps(PipelineConfig config) {
        final Properties props = new Properties();

        // Connection + Schema Registry
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(CFG_KAFKA_BROKER, "localhost:9092"));
        props.put(CFG_SCHEMA_REGISTRY_URL, config.getString(CFG_SCHEMA_REGISTRY_URL, DEFAULT_SCHEMA_REGISTRY_URL));

        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Client id
        final String pipelineName = config.getString(CFG_PIPELINE_NAME, "StreamKernel");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                config.getString(CFG_PRODUCER_CLIENT_ID, pipelineName + "-KAFKA-AVRO"));

        // Durability-by-default
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                config.getString(CFG_PRODUCER_DELIVERY_TIMEOUT_MS, Integer.toString(DEFAULT_DELIVERY_TIMEOUT_MS)));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                config.getString(CFG_PRODUCER_MAX_IN_FLIGHT, Integer.toString(DEFAULT_MAX_IN_FLIGHT)));

        // Throughput knobs (sink-scoped)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString(CFG_PRODUCER_COMPRESSION, DEFAULT_COMPRESSION));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getString(CFG_PRODUCER_BATCH_SIZE, Integer.toString(DEFAULT_BATCH_SIZE)));
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.getString(CFG_PRODUCER_LINGER_MS, Integer.toString(DEFAULT_LINGER_MS)));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getString(CFG_PRODUCER_BUFFER_MEMORY, Long.toString(DEFAULT_BUFFER_MEMORY)));

        // Security passthrough: kafka.ssl.*, kafka.security.*, kafka.sasl.* -> strip "kafka."
        copySecurityProps(config, props);

        return props;
    }

    @Override
    public void write(PipelinePayload<String> payload) {
        if (payload == null) return;

        final GenericRecord record;
        try {
            record = new GenericData.Record(schema);
            mapper.accept(payload, record);
        } catch (Exception e) {
            metrics.counter(metricSerializeErrors, 1.0);
            logRateLimited("Avro mapping/serialization failed id=" + safeId(payload), e);
            throw new RuntimeException("Avro mapping/serialization failed", e);
        }

        producer.send(new ProducerRecord<>(topic, payload.id(), record), (md, ex) -> {
            if (ex != null) {
                metrics.counter(metricWriteErrors, 1.0);
                logRateLimited("KafkaAvroSink write failed id=" + safeId(payload), ex);
            } else {
                metrics.counter(metricWritten, 1.0);
            }
        });
    }

    @Override
    public void close() {
        try {
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("KafkaAvroSink close failed topic={}", topic, e);
        }
    }

    // ---- Helpers ----

    private void logRateLimited(String context, Throwable ex) {
        final long now = System.currentTimeMillis();
        final long last = lastErrorLogMs.get();

        if (now - last >= errorLogIntervalMs && lastErrorLogMs.compareAndSet(last, now)) {
            final long sup = suppressedErrors.sumThenReset();
            if (sup > 0) {
                log.error("{} (suppressed {} similar errors): {}", context, sup, ex.getMessage());
            } else {
                log.error("{}: {}", context, ex.getMessage());
            }
        } else {
            suppressedErrors.increment();
        }
    }

    private static String safeId(PipelinePayload<?> p) {
        try { return (p == null) ? "null" : String.valueOf(p.id()); } catch (Exception e) { return "unknown"; }
    }

    private static Schema loadSchema(String resourcePath) {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalArgumentException("Schema file not found on classpath: " + resourcePath);
            }
            return new Schema.Parser().parse(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Avro schema: " + resourcePath, e);
        }
    }

    private static void copySecurityProps(PipelineConfig src, Properties dst) {
        for (String key : src.keys()) {
            if (key == null) continue;
            if (key.startsWith("kafka.ssl.") || key.startsWith("kafka.sasl.") || key.startsWith("kafka.security.")) {
                String v = src.getString(key, null);
                if (v == null) continue;
                String realKey = key.substring(6); // strip "kafka."
                dst.put(realKey, v);
            }
        }
    }

    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return def; }
    }
}
