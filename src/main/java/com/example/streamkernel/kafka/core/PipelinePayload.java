package com.example.streamkernel.kafka.core;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * The standard envelope for all data flowing through the pipeline.
 * Using a Record ensures immutability and thread-safety.
 *
 * @param id Unique ID for tracing (Correlation ID) - Critical for DLQ debugging
 * @param data The actual payload (Generic T)
 * @param timestamp Ingestion time
 * @param metadata Headers, Source System info, etc.
 */
public record PipelinePayload<T>(
        String id,
        T data,
        Instant timestamp,
        Map<String, String> metadata
) {

    // -----------------------------------------------------------------------
    // 1. FACTORY METHOD (The "Easy Button")
    // -----------------------------------------------------------------------
    // Allows you to do: PipelinePayload.of("MyData")
    // instead of: new PipelinePayload(UUID..., Instant..., Map...)
    public static <T> PipelinePayload<T> of(T data) {
        return new PipelinePayload<>(
                UUID.randomUUID().toString(), // Auto-generate Tracing ID
                data,
                Instant.now(),                // Auto-timestamp
                Collections.emptyMap()        // Start with empty metadata
        );
    }

    // -----------------------------------------------------------------------
    // 2. TRANSFORMATION HELPER
    // -----------------------------------------------------------------------
    // Allows the Transformer to change the DATA (T -> R)
    // while preserving the ID and Timestamp (Traceability).
    public <R> PipelinePayload<R> withData(R newData) {
        return new PipelinePayload<>(id, newData, timestamp, metadata);
    }
}