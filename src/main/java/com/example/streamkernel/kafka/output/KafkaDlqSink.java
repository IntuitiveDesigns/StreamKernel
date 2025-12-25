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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Durable Kafka DLQ sink (generic input type I).
 *
 * Key: payload.id()
 * Value: String.valueOf(payload.data())
 *
 * Note: If you want DLQ to preserve original bytes (Avro/Protobuf), the right design is to
 * add a pluggable serializer for DLQ; this keeps DLQ usable today without changing core generics.
 */
public final class KafkaDlqSink<I> implements OutputSink<I>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqSink.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;

    private final long errorLogIntervalMs;
    private final AtomicLong lastErrorLogMs = new AtomicLong(0);
    private final LongAdder assumesSuppressed = new LongAdder();

    public KafkaDlqSink(String topic, Properties props) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.producer = new KafkaProducer<>(Objects.requireNonNull(props, "props"));

        this.errorLogIntervalMs = Math.max(100L, parseLong(props.getProperty("streamkernel.dlq.error.log.interval.ms", "1000"), 1000L));
        log.info("KafkaDlqSink created. topic='{}'", this.topic);
    }

    public static KafkaDlqSink<?> fromConfig(PipelineConfig config) {
        String topic = config.getProperty("dlq.topic", "streamkernel-dlq");
        Properties props = buildProducerProps(config);

        props.putIfAbsent("streamkernel.dlq.error.log.interval.ms",
                config.getProperty("streamkernel.dlq.error.log.interval.ms", "1000"));

        return new KafkaDlqSink<>(topic, props);
    }

    private static Properties buildProducerProps(PipelineConfig config) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.broker", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String pipelineName = config.getProperty("pipeline.name", "StreamKernel");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getProperty("dlq.kafka.producer.client.id", pipelineName + "-DLQ"));

        // Durable-by-default DLQ semantics (best practice)
        props.put(ProducerConfig.ACKS_CONFIG, config.getProperty("dlq.kafka.producer.acks", "all"));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.getProperty("dlq.kafka.producer.idempotence", "true"));
        props.put(ProducerConfig.RETRIES_CONFIG, config.getProperty("dlq.kafka.producer.retries", Integer.toString(Integer.MAX_VALUE)));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, config.getProperty("dlq.kafka.producer.delivery.timeout.ms", "120000"));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, config.getProperty("dlq.kafka.producer.max.in.flight", "5"));

        // Throughput knobs
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getProperty("dlq.kafka.producer.compression", "lz4"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(config.getProperty("dlq.kafka.producer.batch.size", "65536")));
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(config.getProperty("dlq.kafka.producer.linger.ms", "5")));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.parseLong(config.getProperty("dlq.kafka.producer.buffer.memory", "33554432")));

        // SECURITY PASSTHROUGH (same as KafkaSink)
        for (String key : config.keys()) {
            if (key.startsWith("kafka.ssl.") || key.startsWith("kafka.security.") || key.startsWith("kafka.sasl.")) {
                String realKey = key.substring(6); // remove "kafka."
                props.put(realKey, config.getProperty(key));
            }
        }

        return props;
    }

    @Override
    public void write(PipelinePayload<I> payload) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, payload.id(), String.valueOf(payload.data()));

        producer.send(record, (metadata, exception) -> {
            if (exception == null) return;

            long now = System.currentTimeMillis();
            long last = lastErrorLogMs.get();
            if (now - last >= errorLogIntervalMs && lastErrorLogMs.compareAndSet(last, now)) {
                long suppressed = assumesSuppressed.sumThenReset();
                log.error("DLQ Kafka send failed. suppressed={} topic={} ex={}", suppressed, topic, exception.getMessage());
            } else {
                assumesSuppressed.increment();
            }
        });
    }

    private static long parseLong(String value, long fallback) {
        try { return Long.parseLong(value.trim()); } catch (Exception e) { return fallback; }
    }

    @Override
    public void close() {
        try { producer.close(Duration.ofSeconds(5)); }
        catch (Exception e) { log.warn("DLQ producer close failed", e); }
    }
}
