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
