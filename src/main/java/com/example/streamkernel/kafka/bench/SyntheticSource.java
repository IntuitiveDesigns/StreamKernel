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

package com.example.streamkernel.kafka.bench;

import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.core.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RING BUFFER SOURCE (Fixed Constructor)
 * Match signature: new SyntheticSource(int size, boolean highEntropy)
 */
public final class SyntheticSource implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(SyntheticSource.class);

    private final List<List<PipelinePayload<String>>> ringBuffer;
    private final int bufferSize;
    private final AtomicInteger bufferIndex = new AtomicInteger(0);

    // Tuning
    private static final int CACHED_BATCH_COUNT = 50;
    private static final int PRE_BAKED_BATCH_SIZE = 4000;

    // --- THE CONSTRUCTOR THE COMPILER IS ASKING FOR ---
    public SyntheticSource(int payloadBytes, boolean highEntropy) {
        log.info("🔥 Pre-allocating Ring Buffer: {} batches of {} items...",
                CACHED_BATCH_COUNT, PRE_BAKED_BATCH_SIZE);
        log.info("🎲 Entropy Mode: {}", highEntropy ? "HIGH (Random)" : "LOW (Compressible)");

        this.ringBuffer = new ArrayList<>(CACHED_BATCH_COUNT);
        this.bufferSize = CACHED_BATCH_COUNT;

        Random random = new Random();
        String lowEntropyData = highEntropy ? null : generateRepeatedString(payloadBytes);

        for (int i = 0; i < CACHED_BATCH_COUNT; i++) {
            List<PipelinePayload<String>> batch = new ArrayList<>(PRE_BAKED_BATCH_SIZE);
            for (int j = 0; j < PRE_BAKED_BATCH_SIZE; j++) {
                String dataBody;
                if (highEntropy) {
                    byte[] noise = new byte[payloadBytes];
                    random.nextBytes(noise);
                    dataBody = new String(noise, java.nio.charset.StandardCharsets.ISO_8859_1);
                } else {
                    dataBody = lowEntropyData;
                }
                batch.add(new PipelinePayload<>(
                        "PREGEN-" + i + "-" + j,
                        dataBody,
                        Instant.now(),
                        Collections.emptyMap()
                ));
            }
            ringBuffer.add(Collections.unmodifiableList(batch));
        }
        log.info("✅ Ring Buffer Ready.");
    }

    // Legacy constructor for backward compatibility (if needed)
    public SyntheticSource(int payloadBytes) {
        this(payloadBytes, false);
    }

    @Override
    public void connect() { }

    @Override
    public void disconnect() { }

    @Override
    public PipelinePayload<String> fetch() {
        return ringBuffer.get(getNextIndex()).get(0);
    }

    @Override
    public List<PipelinePayload<String>> fetchBatch(int ignored) {
        // Always return the pre-baked 4MB batch for maximum speed
        return ringBuffer.get(getNextIndex());
    }

    private int getNextIndex() {
        return bufferIndex.getAndIncrement() % bufferSize;
    }

    private String generateRepeatedString(int size) {
        StringBuilder sb = new StringBuilder(size);
        while (sb.length() < size) {
            sb.append("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        }
        return sb.substring(0, size);
    }
}