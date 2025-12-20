package com.example.streamkernel.kafka.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipelinePayloadTest {

    @Test
    void of_shouldPopulateTimestampAndEmptyMetadata() {
        PipelinePayload<String> payload = PipelinePayload.of("hello");

        assertNotNull(payload.id());
        assertEquals("hello", payload.data());
        assertNotNull(payload.timestamp());
        assertTrue(payload.metadata().isEmpty());
    }

    @Test
    void withData_shouldPreserveIdTimestampAndMetadata() {
        Instant ts = Instant.now();
        Map<String, String> meta = Map.of("source", "test");

        PipelinePayload<String> original =
                new PipelinePayload<>("id-123", "original", ts, meta);

        PipelinePayload<String> updated = original.withData("updated");

        // id + timestamp + metadata must be preserved
        assertEquals("id-123", updated.id());
        assertEquals(ts, updated.timestamp());
        assertEquals(meta, updated.metadata());

        // but data should be changed
        assertEquals("updated", updated.data());
    }
}
