/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class StreamKernelProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamKernelProducer.class);

    private static final String DEFAULT_PROPERTIES_FILE = "producer.properties";
    private static final String P_PROPERTIES_FILE = "producer.properties.file";

    private static final String P_TOPIC_DEFAULT = "producer.topic.default";
    private static final String DEFAULT_TOPIC = "streamkernel-test-topic";

    private final KafkaProducer<String, String> producer;

    public StreamKernelProducer() {
        this(loadProperties(resolvePropertiesFile()));
    }

    public StreamKernelProducer(Properties props) {
        Objects.requireNonNull(props, "props");
        // Safe defaults if config file is minimal
        props.putIfAbsent("key.serializer", StringSerializer.class.getName());
        props.putIfAbsent("value.serializer", StringSerializer.class.getName());

        // Optimizations for throughput (override via properties file if needed)
        props.putIfAbsent("acks", "all");

        this.producer = new KafkaProducer<>(props);
    }

    // --- Core Operations ---

    /**
     * Synchronous send. Blocks until the broker acknowledges the write.
     * Useful for validation, not for high throughput.
     */
    public RecordMetadata sendMessage(String topic, String key, String value) {
        Objects.requireNonNull(topic, "topic");

        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        try {
            final long startNs = log.isDebugEnabled() ? System.nanoTime() : 0;

            final RecordMetadata md = producer.send(record).get(); // Blocking call

            if (log.isDebugEnabled()) {
                final long elapsedUs = (System.nanoTime() - startNs) / 1_000;
                log.debug("Sent (Sync): topic={} part={} off={} time={}us",
                        md.topic(), md.partition(), md.offset(), elapsedUs);
            }
            return md;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for producer ack", ie);
        } catch (ExecutionException ee) {
            throw new RuntimeException("Producer send failed", ee.getCause());
        }
    }

    /**
     * Asynchronous send. Returns immediately.
     * @return A Future that can be ignored or checked later.
     */
    public Future<RecordMetadata> sendAsync(String topic, String key, String value) {
        return sendAsync(topic, key, value, null);
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, String value, Callback callback) {
        Objects.requireNonNull(topic, "topic");
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        return producer.send(record, (md, ex) -> {
            // Internal logging callback
            if (ex != null) {
                log.error("Send failed: topic={} key={}", topic, key, ex);
            } else if (log.isTraceEnabled()) {
                log.trace("Sent (Async): topic={} off={}", md.topic(), md.offset());
            }
            // Chain external callback if provided
            if (callback != null) {
                callback.onCompletion(md, ex);
            }
        });
    }

    /**
     * Blocking flush. Waits for all buffered records to be sent.
     */
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        log.info("Closing StreamKernelProducer...");
        // Close with a short timeout to prevent hanging the JVM on exit
        producer.close(Duration.ofSeconds(5));
    }

    // --- Configuration Helpers ---

    private static String resolvePropertiesFile() {
        return normalize(System.getProperty(P_PROPERTIES_FILE), DEFAULT_PROPERTIES_FILE);
    }

    private static Properties loadProperties(String resourceName) {
        final Properties props = new Properties();
        // Use the current thread's loader to be safe in containers/frameworks
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) cl = StreamKernelProducer.class.getClassLoader();

        try (InputStream input = cl.getResourceAsStream(resourceName)) {
            if (input == null) {
                // Not fatal: user might rely on programmatic props, but warn them.
                log.warn("Configuration file '{}' not found in classpath. Using defaults.", resourceName);
                return props;
            }
            props.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to load producer config: " + resourceName, ex);
        }
        return props;
    }

    private static String normalize(String value, String def) {
        if (value == null) return def;
        String v = value.trim();
        return v.isEmpty() ? def : v;
    }

    // --- CLI Entry Point ---

    public static void main(String[] args) {
        final String topic = (args.length > 0 && args[0] != null && !args[0].isBlank())
                ? args[0].trim()
                : normalize(System.getProperty(P_TOPIC_DEFAULT), DEFAULT_TOPIC);

        System.out.println("🚀 StreamKernelProducer: Connecting to " + topic + "...");

        // Try-with-resources handles shutdown automatically
        try (StreamKernelProducer p = new StreamKernelProducer()) {
            for (int i = 0; i < 5; i++) {
                String payload = "Validating connectivity sequence #" + i;
                p.sendMessage(topic, "key-" + i, payload);
                System.out.println("   ✅ Sent message " + i);
            }
            System.out.println("Flushing buffer...");
            p.flush();
        } catch (Exception e) {
            System.err.println("❌ Producer failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("Done.");
    }
}