package com.example.streamkernel.kafka.ingestion;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.core.SourceConnector;
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

public class KafkaSourceConnector implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceConnector.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final Queue<PipelinePayload<String>> buffer = new ConcurrentLinkedQueue<>();
    private volatile boolean running = false;

    public KafkaSourceConnector(String topic, Properties props) {
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(props);
    }

    /**
     * Factory method to create a Source with Security/SSL headers automatically injected.
     */
    public static KafkaSourceConnector fromConfig(PipelineConfig config) {
        String topic = Objects.requireNonNull(config.getProperty("source.kafka.topic"), "source.kafka.topic is required");
        Properties props = buildConsumerProps(config);
        return new KafkaSourceConnector(topic, props);
    }

    private static Properties buildConsumerProps(PipelineConfig config) {
        Properties props = new Properties();

        // Basics
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.broker", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("kafka.consumer.group", "streamkernel-group"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getProperty("kafka.consumer.auto.offset.reset", "latest"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getProperty("kafka.consumer.auto.commit", "true"));

        // --- SECURITY PASSTHROUGH (mTLS / SASL) ---
        // Mirroring the logic from KafkaSink to support SSL
        for (String key : config.keys()) {
            if (key.startsWith("kafka.ssl.") || key.startsWith("kafka.security.") || key.startsWith("kafka.sasl.")) {
                String realKey = key.substring(6); // remove "kafka."
                props.put(realKey, config.getProperty(key));
            }
        }
        return props;
    }

    @Override
    public void connect() {
        log.info("🔌 Connecting to Kafka Topic: {}", topic);
        consumer.subscribe(Collections.singletonList(topic));
        running = true;
    }

    @Override
    public void disconnect() {
        log.info("🔌 Disconnecting Kafka Consumer...");
        running = false;
        try {
            consumer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("Error closing consumer", e);
        }
    }

    @Override
    public PipelinePayload<String> fetch() {
        if (!buffer.isEmpty()) {
            return buffer.poll();
        }

        // Poll Kafka
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) return null;

            for (ConsumerRecord<String, String> record : records) {
                Map<String, String> metadata = new HashMap<>();
                metadata.put("kafka.partition", String.valueOf(record.partition()));
                metadata.put("kafka.offset", String.valueOf(record.offset()));
                if (record.key() != null) metadata.put("kafka.key", record.key());

                buffer.add(new PipelinePayload<>(
                        UUID.randomUUID().toString(),
                        record.value(),
                        Instant.ofEpochMilli(record.timestamp()),
                        metadata
                ));
            }
        } catch (Exception e) {
            log.error("Error during poll", e);
            return null;
        }

        return buffer.poll();
    }
}