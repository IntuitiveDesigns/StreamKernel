package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.core.PipelinePayload;

/**
 * Produces key/value bytes for DLQ persistence.
 *
 * Design goals:
 *  - Zero/low allocation on the hot path where possible
 *  - Works for any payload type I
 *  - Allows future DLQ formats (Avro/Protobuf/JSON) without changing orchestrator
 */
public interface DlqSerializer<I> {
    String id(); // e.g. "STRING", "JSON", "AVRO_GENERIC", "AVRO_SPECIFIC"
    byte[] key(PipelinePayload<I> payload);
    byte[] value(PipelinePayload<I> payload);
}
