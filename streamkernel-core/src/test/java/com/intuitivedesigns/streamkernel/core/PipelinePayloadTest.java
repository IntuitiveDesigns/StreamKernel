/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.core;

import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipelinePayloadTest {

    @Test
    void testImmutability() {
        // Setup
        String id = "event-123";
        String data = "{\"value\": 100}";
        Map<String, String> meta = Map.of("origin", "kafka");

        // Act
        PipelinePayload<String> payload = new PipelinePayload<>(id, data, meta);

        // Assert
        assertEquals(id, payload.id());
        assertEquals(data, payload.data());

        // If your class is a Java Record, use .metadata().
        // If it is a POJO, use .getMetadata() or .headers().
        // Below assumes a Record-style accessor which is common in this project.
        // If this fails, change to .getMetadata()
        assertEquals(meta, payload.metadata());
    }

    @Test
    void testWithMetadata() {
        PipelinePayload<String> original = new PipelinePayload<>("id", "data", Map.of());

        Map<String, String> newMeta = Map.of("processed", "true");
        PipelinePayload<String> updated = original.withMetadata(newMeta);

        assertNotSame(original, updated); // Must be a new object (Immutability)
        assertEquals(newMeta, updated.metadata());
    }
}