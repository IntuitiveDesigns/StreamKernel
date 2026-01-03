/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.bench;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A high-performance synthetic data generator for benchmarking.
 */
public final class SyntheticSource implements SourceConnector<String> {

    private final List<PipelinePayload<String>> ringBuffer;
    private final int ringSize;
    private final AtomicLong cursor = new AtomicLong(0);

    public SyntheticSource(PipelineConfig config, MetricsRuntime metrics) {
        this(
            config.getInt("synthetic.payload.bytes", 1024),
            config.getBoolean("synthetic.high.entropy", false)
        );
    }

    public SyntheticSource(int payloadBytes, boolean highEntropy) {
        this.ringSize = 1000;
        this.ringBuffer = new ArrayList<>(ringSize);
        
        byte[] buffer = new byte[payloadBytes];
        Random r = new Random(12345); 

        for (int i = 0; i < ringSize; i++) {
            if (highEntropy) {
                r.nextBytes(buffer);
            } else {
                for (int j = 0; j < payloadBytes; j++) buffer[j] = (byte) 'A';
            }
            String data = new String(buffer, StandardCharsets.UTF_8);
            ringBuffer.add(PipelinePayload.of(data));
        }
    }

    // --- INTERFACE IMPLEMENTATION ---
    @Override
    public void connect() {
        // No-op for synthetic source (always connected)
    }

    @Override
    public PipelinePayload<String> fetch() {
        int idx = (int) (cursor.getAndIncrement() % ringSize);
        if (idx < 0) idx = -idx; 
        return ringBuffer.get(idx);
    }

    // Note: fetchBatch is likely a default method in the interface, or not required.
    public List<PipelinePayload<String>> fetchBatch(int maxBatchSize) {
        List<PipelinePayload<String>> batch = new ArrayList<>(maxBatchSize);
        for (int i = 0; i < maxBatchSize; i++) {
            batch.add(fetch());
        }
        return batch;
    }

    @Override
    public void disconnect() {
        // No-op
    }
}
