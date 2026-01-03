/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.kafka;

import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.bench.SyntheticSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Source-only burn test to measure max fetchBatch throughput.
 * All values can be overridden via -D system properties.
 */
public final class SourceBurnTest {

    // System properties
    private static final String P_PAYLOAD_BYTES = "bench.payload.bytes";
    private static final String P_HIGH_ENTROPY = "bench.high.entropy";
    private static final String P_BATCH = "bench.batch";
    private static final String P_REPORT_EVERY = "bench.report.every";
    private static final String P_REPORT_MS = "bench.report.ms";

    // Defaults
    private static final int DEFAULT_PAYLOAD_BYTES = 1024;
    private static final boolean DEFAULT_HIGH_ENTROPY = false;
    private static final int DEFAULT_BATCH = 4000;
    private static final long DEFAULT_REPORT_EVERY = 10_000_000L;
    private static final long DEFAULT_REPORT_MS = 1_000L;

    private SourceBurnTest() {}

    public static void main(String[] args) throws InterruptedException {
        final int payloadBytes = clampInt(getInt(P_PAYLOAD_BYTES, DEFAULT_PAYLOAD_BYTES), 0, Integer.MAX_VALUE);
        final boolean highEntropy = getBoolean(P_HIGH_ENTROPY, DEFAULT_HIGH_ENTROPY);
        final int batchSize = clampInt(getInt(P_BATCH, DEFAULT_BATCH), 1, Integer.MAX_VALUE);
        final long reportEvery = clampLong(getLong(P_REPORT_EVERY, DEFAULT_REPORT_EVERY), 1L, Long.MAX_VALUE);
        final long reportMs = clampLong(getLong(P_REPORT_MS, DEFAULT_REPORT_MS), 250L, 60_000L);

        final SourceConnector<String> source = new SyntheticSource(payloadBytes, highEntropy);

        System.out.println("🔥 Starting Source Burn Test...");
        System.out.printf("CONFIG: payloadBytes=%d entropy=%s batch=%d reportEvery=%d reportMs=%d%n",
                payloadBytes, highEntropy ? "HIGH" : "LOW", batchSize, reportEvery, reportMs);

        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch stopLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!running.compareAndSet(true, false)) return;
            System.out.println("Shutdown signal received.");
            stopLatch.countDown();
        }, "source-burn-shutdown"));

        final LongAdder counter = new LongAdder();

        // Reporting state
        long lastReportCount = 0L;
        long lastReportNs = System.nanoTime();

        while (running.get()) {
            var batch = source.fetchBatch(batchSize);
            counter.add(batch.size());

            // Check triggers efficiently (avoid expensive System.nanoTime() on every loop if possible)
            final long currentTotal = counter.sum();
            final long deltaCount = currentTotal - lastReportCount;

            boolean triggerReport = false;

            // 1. Volume Trigger
            if (deltaCount >= reportEvery) {
                triggerReport = true;
            }
            // 2. Time Trigger (Only check every N iterations to save CPU?
            //    No, for a benchmark we want precision. System.nanoTime is cheap enough on modern JVMs)
            else {
                long nowNs = System.nanoTime();
                if (TimeUnit.NANOSECONDS.toMillis(nowNs - lastReportNs) >= reportMs) {
                    triggerReport = true;
                }
            }

            if (triggerReport) {
                final long nowNs = System.nanoTime();
                final double seconds = (nowNs - lastReportNs) / 1_000_000_000.0;
                final double eps = seconds <= 0 ? 0.0 : deltaCount / seconds;

                System.out.printf("SPEED: %,.0f Events/Sec | Total: %,d%n", eps, currentTotal);

                // Reset window for "Instantaneous Speed" accuracy
                lastReportCount = currentTotal;
                lastReportNs = nowNs;
            }
        }

        stopLatch.await();
    }

    private static boolean getBoolean(String key, boolean def) {
        String v = System.getProperty(key);
        return v == null ? def : Boolean.parseBoolean(v.trim());
    }

    private static int getInt(String key, int def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        if (v.isEmpty()) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }

    private static long getLong(String key, long def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        if (v.isEmpty()) return def;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return def; }
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }
}