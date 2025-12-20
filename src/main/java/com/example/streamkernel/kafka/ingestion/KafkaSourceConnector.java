package com.example.streamkernel.kafka.ingestion;

import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.core.SourceConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    // Internal buffer to smooth out the "Batch Poll" into "Single Item Fetch"
    private final Queue<PipelinePayload<String>> buffer = new ConcurrentLinkedQueue<>();
    private volatile boolean running = false;

    public KafkaSourceConnector(String topic, Properties props) {
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(props);
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
        consumer.close();
    }

    @Override
    public PipelinePayload<String> fetch() {
        // 1. If we have data buffered from the last poll, return it immediately
        if (!buffer.isEmpty()) {
            return buffer.poll();
        }

        // 2. If buffer is empty, POLL Kafka for more data
        // We poll for 100ms. If nothing comes, we return null (Orchestrator handles the sleep)
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        if (records.isEmpty()) {
            return null;
        }

        // 3. Convert Kafka Records -> PipelinePayloads and fill buffer
        for (ConsumerRecord<String, String> record : records) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("kafka.partition", String.valueOf(record.partition()));
            metadata.put("kafka.offset", String.valueOf(record.offset()));
            metadata.put("kafka.key", record.key());

            // Wrap it in our standard envelope
            buffer.add(new PipelinePayload<>(
                    UUID.randomUUID().toString(), // Or use record.key() as ID
                    record.value(),
                    Instant.ofEpochMilli(record.timestamp()),
                    metadata
            ));
        }

        return buffer.poll();
    }
}