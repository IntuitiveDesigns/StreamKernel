package com.example.streamkernel.kafka.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A pluggable source of events for the pipeline.
 *
 * Examples:
 * - Kafka topic consumer
 * - REST API poller
 * - S3 object reader
 * - Database change feed
 *
 * Contract:
 * - {@link #connect()} is called once before the pipeline starts reading.
 * - {@link #fetch()} is called repeatedly in a loop by the orchestrator.
 * - {@link #disconnect()} is called once when the pipeline stops.
 *
 * Implementations should prefer:
 * - Short, bounded blocking in {@link #fetch()} (but NOT infinite blocking)
 * - Returning {@code null} when no data is currently available
 * so the orchestrator can back off briefly and try again.
 *
 * @param <T> Raw data type produced by this source.
 */
public interface SourceConnector<T> {

    /**
     * Initialize and connect to the underlying system.
     */
    void connect();

    /**
     * Clean up any opened resources.
     */
    void disconnect();

    /**
     * Fetch a single payload from the source.
     *
     * @return the next {@link PipelinePayload}, or {@code null} if no data is available.
     */
    PipelinePayload<T> fetch();

    /**
     * Fetch a batch of payloads from the source.
     * <p>
     * <b>Performance Note:</b> This method is the key to high throughput.
     * The default implementation calls {@link #fetch()} in a loop, which reduces
     * orchestrator overhead (locking/threading) but does not reduce I/O overhead.
     * <p>
     * <b>Recommendation:</b> Override this in your KafkaSourceConnector to return
     * the raw list from {@code consumer.poll()} directly.
     *
     * @param maxBatchSize The maximum number of records to return.
     * @return A list of payloads. Returns an empty list or null if no data is available.
     */
    default List<PipelinePayload<T>> fetchBatch(int maxBatchSize) {
        // Optimize allocation to avoid resizing overhead
        List<PipelinePayload<T>> batch = new ArrayList<>(maxBatchSize);

        for (int i = 0; i < maxBatchSize; i++) {
            PipelinePayload<T> payload = fetch();
            if (payload == null) {
                // If we run out of data, stop filling the batch and return what we have.
                // We do NOT block waiting for a full batch.
                break;
            }
            batch.add(payload);
        }

        return batch;
    }
}