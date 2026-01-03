/*
 * Copyright 2026 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.bench;

import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-Performance Ring Buffer Source.
 *
 * <p><b>Performance Characteristics:</b>
 * <ul>
 * <li><b>Zero-Allocation (Payload objects):</b> Reuses pre-generated {@code PipelinePayload<String>} instances.</li>
 * <li><b>Steady-State allocations:</b>
 * <ul>
 * <li><b>Safe mode (default):</b> allocates a new {@link ArrayList} per {@link #fetchBatch(int)} call.</li>
 * <li><b>Benchmark mode (unsafeReuseBatch=true):</b> reuses a ThreadLocal {@link ArrayList} to avoid allocations.
 * Caller must consume synchronously and must not retain the returned list reference.</li>
 * </ul>
 * </li>
 * <li><b>Concurrency:</b> Lock-free indexing via {@link AtomicLong} plus power-of-two masking.
 * Note: contention may rise at very high thread counts.</li>
 * </ul>
 */
public final class SyntheticSource implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(SyntheticSource.class);

    private static final int DEFAULT_BUFFER_COUNT = 262_144;      // 2^18
    private static final int MAX_BUFFER_CAPACITY = 1 << 30;       // practical upper bound to avoid overflow
    private static final int DEFAULT_RECYCLED_LIST_CAP = 8_192;   // better default for 5k-20k batches

    private final PipelinePayload<String>[] ringBuffer;
    private final int mask;
    private final AtomicLong sequence = new AtomicLong(0);

    private final boolean unsafeReuseBatch;
    private final ThreadLocal<ArrayList<PipelinePayload<String>>> recycledBatch;

    /**
     * Legacy constructor (safe mode) for compatibility with existing plugin factories.
     */
    public SyntheticSource(int payloadSize, boolean highEntropy) {
        this(payloadSize, DEFAULT_BUFFER_COUNT, highEntropy, false);
    }

    /**
     * Full constructor.
     *
     * @param payloadSize      fixed payload length in characters (String.length()).
     * @param bufferCount      requested ring buffer size (rounded up to power-of-two).
     * @param highEntropy      if true, uses canonical SplitMix64 to generate deterministic pseudo-random content.
     * @param unsafeReuseBatch if true, reuses a per-thread ArrayList in fetchBatch() (fast but unsafe for async pipelines).
     */
    @SuppressWarnings("unchecked")
    public SyntheticSource(int payloadSize, int bufferCount, boolean highEntropy, boolean unsafeReuseBatch) {
        if (payloadSize <= 0) throw new IllegalArgumentException("payloadSize must be positive");
        if (bufferCount <= 0) throw new IllegalArgumentException("bufferCount must be positive");
        if (bufferCount > MAX_BUFFER_CAPACITY) throw new IllegalArgumentException("bufferCount exceeds limit (1<<30)");

        int capacity = nextPowerOfTwoOrThrow(bufferCount);
        this.mask = capacity - 1;
        this.unsafeReuseBatch = unsafeReuseBatch;

        this.ringBuffer = (PipelinePayload<String>[]) new PipelinePayload[capacity];
        this.recycledBatch = unsafeReuseBatch
                ? ThreadLocal.withInitial(() -> new ArrayList<>(DEFAULT_RECYCLED_LIST_CAP))
                : null;

        log.info("Initializing SyntheticSource: capacity={} (requested={}) payloadChars={} entropyMode={} unsafeReuseBatch={}",
                capacity, bufferCount, payloadSize, highEntropy ? "HIGH" : "LOW", unsafeReuseBatch);

        for (int i = 0; i < capacity; i++) {
            String payload = highEntropy
                    ? generateSplitMix64String(payloadSize, i)
                    : generateLowEntropyString(payloadSize, i);

            ringBuffer[i] = PipelinePayload.of(payload);
        }

        log.info("SyntheticSource initialization complete.");
    }

    @Override
    public void connect() {
        // no-op
    }

    @Override
    public void disconnect() {
        // no-op
    }

    @Override
    public PipelinePayload<String> fetch() {
        long seq = sequence.getAndIncrement();
        return ringBuffer[(int) (seq & mask)];
    }

    @Override
    public List<PipelinePayload<String>> fetchBatch(int maxBatchSize) {
        if (maxBatchSize <= 0) return Collections.emptyList();

        final List<PipelinePayload<String>> batch;
        if (unsafeReuseBatch) {
            // Benchmark mode: reuse list to minimize allocations.
            // IMPORTANT: Caller must consume synchronously and must not retain the returned list reference.
            ArrayList<PipelinePayload<String>> reusable = recycledBatch.get();
            reusable.clear();
            reusable.ensureCapacity(maxBatchSize);
            batch = reusable;
        } else {
            // Safe enterprise mode: allocate a new list per call.
            batch = new ArrayList<>(maxBatchSize);
        }

        long start = sequence.getAndAdd(maxBatchSize);
        for (int i = 0; i < maxBatchSize; i++) {
            int index = (int) ((start + i) & mask);
            batch.add(ringBuffer[index]);
        }

        return batch;
    }

    // -------------------------
    // Helpers
    // -------------------------

    private static int nextPowerOfTwoOrThrow(int value) {
        // value is positive and <= MAX_BUFFER_CAPACITY
        int highestOneBit = Integer.highestOneBit(value);
        if (value == highestOneBit) return value;

        // shift would overflow if highestOneBit is already 1<<30 (but we validate bufferCount <= 1<<30)
        int next = highestOneBit << 1;
        if (next <= 0 || next > MAX_BUFFER_CAPACITY) {
            throw new IllegalArgumentException("bufferCount too large to round to next power-of-two safely: " + value);
        }
        return next;
    }

    private static String generateLowEntropyString(int len, int slot) {
        // Format: "E<HEX>:" + 'X' padding, fixed length == len
        char[] chars = new char[len];
        int pos = 0;

        if (len > 0) chars[pos++] = 'E';
        pos = writeHexIntUpper(slot, chars, pos);
        if (pos < len) chars[pos++] = ':';

        while (pos < len) {
            chars[pos++] = 'X';
        }
        return new String(chars);
    }

    /**
     * Zero-allocation hex writer (uppercase).
     */
    private static int writeHexIntUpper(int value, char[] out, int pos) {
        int remaining = out.length - pos;
        if (remaining <= 0) return pos;

        boolean started = false;
        for (int shift = 28; shift >= 0 && remaining > 0; shift -= 4) {
            int nibble = (value >>> shift) & 0xF;
            if (!started) {
                if (nibble == 0 && shift != 0) continue;
                started = true;
            }
            out[pos++] = (char) (nibble < 10 ? ('0' + nibble) : ('A' + (nibble - 10)));
            remaining--;
        }
        if (!started && remaining > 0) {
            out[pos++] = '0';
        }
        return pos;
    }

    private static String generateSplitMix64String(int len, int slot) {
        // Canonical SplitMix64: state += gamma; mix(state)
        char[] chars = new char[len];
        long state = 0x9E3779B97F4A7C15L ^ (long) slot;

        for (int i = 0; i < len; i++) {
            state += 0x9E3779B97F4A7C15L;

            long z = state;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            z = z ^ (z >>> 31);

            chars[i] = mapToBase64ish((int) (z & 63));
        }

        return new String(chars);
    }

    private static char mapToBase64ish(int v) {
        // 0..63 -> [A-Z][a-z][0-9]+/
        if (v < 26) return (char) ('A' + v);
        if (v < 52) return (char) ('a' + (v - 26));
        if (v < 62) return (char) ('0' + (v - 52));
        return (v == 62) ? '+' : '/';
    }
}
