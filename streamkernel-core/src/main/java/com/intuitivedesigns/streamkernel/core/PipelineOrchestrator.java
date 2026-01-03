/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.core;

import com.intuitivedesigns.streamkernel.spi.Cache;

import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * Improvements:
 * - Uses Immutable AuthCache pattern (from v6.2) for atomic security state.
 * - Uses Spin-Wait Draining (from v6.1) to eliminate Latch race conditions.
 * - Proper volatile visibility for lazily initialized components.
 */
public class PipelineOrchestrator<I, O> {

    private static final Logger log = LoggerFactory.getLogger(PipelineOrchestrator.class);

    // Core Components
    private final SourceConnector<I> source;
    private final OutputSink<O> primarySink;
    private final OutputSink<I> dlqSink;
    private final Transformer<I, O> transformer;
    private final Cache<?, ?> cache;
    private final LongAdder meter;

    // Security Components
    private final SecurityProvider securityProvider;
    private final String serviceAccount;
    private final String targetResource;
    private final String action;

    // Tuning
    private final int parallelism;
    private final int batchSize;
    private final boolean authorizePerRecord;
    private final long authTtlNanos;
    private final boolean failFastOnSourceError;
    private final long sourceErrorBackoffInitialMs;
    private final long sourceErrorBackoffMaxMs;

    // Runtime State (Volatile is required because they are set in start(), not constructor)
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ExecutorService executor;
    private volatile Thread dispatcherThread;
    private volatile Semaphore concurrencyLimiter;

    // Draining
    private final AtomicInteger inFlightBatches = new AtomicInteger(0);

    // Counters
    private final LongAdder deniedCount = new LongAdder();
    private final LongAdder dlqCount = new LongAdder();
    private final LongAdder sourceErrorCount = new LongAdder();
    private final LongAdder authErrorCount = new LongAdder();

    // --- BEST PRACTICE: Immutable State Holder for Volatile Data ---
    // This ensures that 'expiresAt' and 'allowed' are always read together consistently.
    private static final class AuthCache {
        final long expiresAtNanos;
        final boolean allowed;

        AuthCache(long expiresAtNanos, boolean allowed) {
            this.expiresAtNanos = expiresAtNanos;
            this.allowed = allowed;
        }
    }
    // Initialize with expired cache
    private volatile AuthCache authCache = new AuthCache(0L, false);
    private volatile long lastAuthErrorLogAtMs = 0L;

    public PipelineOrchestrator(SourceConnector<I> source,
                                OutputSink<O> primarySink,
                                OutputSink<I> dlqSink,
                                Transformer<I, O> transformer,
                                Cache<?, ?> cache,
                                LongAdder meter,
                                int parallelism,
                                int batchSize,
                                SecurityProvider securityProvider,
                                String serviceAccount,
                                String targetResource,
                                String action,
                                boolean authorizePerRecord,
                                long authTtlNanos,
                                boolean failFastOnSourceError,
                                long sourceErrorBackoffInitialMs,
                                long sourceErrorBackoffMaxMs) {

        this.source = Objects.requireNonNull(source, "source");
        this.primarySink = Objects.requireNonNull(primarySink, "primarySink");
        this.dlqSink = Objects.requireNonNull(dlqSink, "dlqSink");
        this.transformer = Objects.requireNonNull(transformer, "transformer");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.securityProvider = Objects.requireNonNull(securityProvider, "securityProvider");

        this.meter = meter;
        this.serviceAccount = (serviceAccount != null && !serviceAccount.isBlank()) ? serviceAccount : "unknown-service";
        this.targetResource = (targetResource != null && !targetResource.isBlank()) ? targetResource : "unknown-resource";
        this.action = (action != null && !action.isBlank()) ? action : "write";

        if (parallelism <= 0) throw new IllegalArgumentException("Parallelism must be > 0");
        if (batchSize <= 0) throw new IllegalArgumentException("BatchSize must be > 0");
        this.parallelism = parallelism;
        this.batchSize = batchSize;

        this.authorizePerRecord = authorizePerRecord;
        this.authTtlNanos = Math.max(0L, authTtlNanos);

        this.failFastOnSourceError = failFastOnSourceError;
        this.sourceErrorBackoffInitialMs = Math.max(0L, sourceErrorBackoffInitialMs);
        this.sourceErrorBackoffMaxMs = Math.max(this.sourceErrorBackoffInitialMs, sourceErrorBackoffMaxMs);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;

        log.info("Starting pipeline components...");
        source.connect();

        this.concurrencyLimiter = new Semaphore(parallelism, false); // Non-fair for throughput
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        this.dispatcherThread = new Thread(this::runDispatcherLoop, "pipeline-dispatcher");
        this.dispatcherThread.setDaemon(true);
        this.dispatcherThread.start();

        log.info("Pipeline Started: parallelism={} batchSize={} user={} resource={}",
                parallelism, batchSize, serviceAccount, targetResource);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        log.info("Stop requested. Draining in-flight batches...");

        // 1. Interrupt Dispatcher (Stops fetching new work)
        Thread dt = dispatcherThread;
        if (dt != null) dt.interrupt();

        // 2. Safe Drain Loop (Spin-Wait)
        // This is safer than CountDownLatch because inFlightBatches is the dynamic truth.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

        while (inFlightBatches.get() > 0 && System.nanoTime() < deadline) {
            Thread.onSpinWait(); // CPU hint to save power while spinning
            try {
                Thread.sleep(10); // Minimal latency sleep
            } catch (InterruptedException ignored) {
                break;
            }
        }

        // 3. Hard Stop Executor
        ExecutorService ex = executor;
        if (ex != null) {
            ex.shutdownNow();
            try { ex.awaitTermination(5, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }

        // 4. Close Resources
        safeClose(primarySink, "primarySink");
        safeClose(dlqSink, "dlqSink");
        safeDisconnectSource();

        log.info("Pipeline Stopped. remaining={} denied={} dlq={} errors={} authErrors={}",
                inFlightBatches.get(), deniedCount.sum(), dlqCount.sum(), sourceErrorCount.sum(), authErrorCount.sum());
    }

    private void runDispatcherLoop() {
        long backoffMs = sourceErrorBackoffInitialMs;

        try {
            while (running.get()) {
                concurrencyLimiter.acquire();

                List<PipelinePayload<I>> batch;
                try {
                    batch = source.fetchBatch(batchSize);
                    backoffMs = sourceErrorBackoffInitialMs;
                } catch (Exception e) {
                    concurrencyLimiter.release();
                    handleSourceError(e, backoffMs);
                    if (failFastOnSourceError) break;
                    backoffMs = Math.min(backoffMs * 2, sourceErrorBackoffMaxMs);
                    continue;
                }

                if (batch == null || batch.isEmpty()) {
                    concurrencyLimiter.release();
                    // Avoid tight loop burn
                    try { Thread.sleep(5); } catch (InterruptedException ie) { break; }
                    continue;
                }

                inFlightBatches.incrementAndGet();

                // Fire and forget
                executor.submit(() -> processBatchTask(batch));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            log.error("Dispatcher crashed", t);
            running.set(false);
        }
    }

    private void processBatchTask(List<PipelinePayload<I>> batch) {
        try {
            if (batch.size() <= batchSize) {
                processBatch(batch);
            } else {
                processPossiblyOversizedBatch(batch);
            }
        } finally {
            concurrencyLimiter.release();
            if (meter != null) meter.add(batch.size());
            inFlightBatches.decrementAndGet();
        }
    }

    private void processPossiblyOversizedBatch(List<PipelinePayload<I>> batch) {
        final int n = batch.size();
        for (int start = 0; start < n; start += batchSize) {
            final int end = Math.min(start + batchSize, n);
            processBatch(batch.subList(start, end));
        }
    }

    private void processBatch(List<PipelinePayload<I>> batch) {
        // 1. Batch Auth
        if (!authorizePerRecord) {
            if (!isAllowedCachedFailClosed()) {
                deniedCount.add(batch.size());
                log.warn("SECURITY DENIAL (Batch): user='{}' action='{}'", serviceAccount, action);
                for (PipelinePayload<I> p : batch) {
                    safeCachePut(p);
                    safeDlqWrite(p);
                }
                return;
            }
        }

        // 2. Record Processing (Stack Hoisting)
        final OutputSink<O> sinkRef = this.primarySink;
        final Transformer<I, O> transRef = this.transformer;
        final boolean authPerRec = this.authorizePerRecord;

        for (PipelinePayload<I> payload : batch) {
            try {
                safeCachePut(payload);

                if (authPerRec && !isAllowedFailClosed()) {
                    deniedCount.increment();
                    safeDlqWrite(payload);
                    continue;
                }

                PipelinePayload<O> result = transRef.transform(payload);
                sinkRef.write(result);
            } catch (Exception e) {
                log.warn("Record failed id={}: {}", payload.id(), e.getMessage());
                safeDlqWrite(payload);
            }
        }
    }

    // --- Security Logic (Hybrid) ---

    private boolean isAllowedCachedFailClosed() {
        if (authTtlNanos <= 0) return isAllowedFailClosed();

        final long now = System.nanoTime();

        // SAFE: Atomic read of the immutable tuple
        final AuthCache local = this.authCache;

        if (now < local.expiresAtNanos) {
            return local.allowed;
        }

        final boolean allowed = isAllowedFailClosed();

        // SAFE: Atomic write of the new state
        this.authCache = new AuthCache(now + authTtlNanos, allowed);
        return allowed;
    }

    private boolean isAllowedFailClosed() {
        try {
            return securityProvider.isAllowed(serviceAccount, action, targetResource);
        } catch (Exception e) {
            authErrorCount.increment();
            final long nowMs = System.currentTimeMillis();
            if (nowMs - lastAuthErrorLogAtMs > 5_000L) {
                lastAuthErrorLogAtMs = nowMs;
                log.error("Security Provider Error. Denying. user='{}' msg='{}'", serviceAccount, e.getMessage());
            }
            return false;
        }
    }

    // --- Helpers ---

    private void handleSourceError(Exception e, long backoffMs) throws InterruptedException {
        sourceErrorCount.increment();
        if (!failFastOnSourceError) {
            log.error("Source fetch failed (retrying in {}ms)", backoffMs, e);
            if (backoffMs > 0) Thread.sleep(backoffMs);
        } else {
            log.error("Source fetch failed (Fail-Fast)", e);
        }
    }

    private void safeCachePut(PipelinePayload<I> p) {
        try { ((Cache) cache).put(p.id(), p); } catch (Exception ignored) {}
    }

    private void safeDlqWrite(PipelinePayload<I> p) {
        try {
            dlqSink.write(p);
            dlqCount.increment();
        } catch (Exception e) {
            log.error("DOUBLE FAULT: DLQ write failed id={}", p.id(), e);
        }
    }

    private void safeClose(Object o, String name) {
        try { if (o instanceof AutoCloseable c) c.close(); } catch (Exception e) { log.warn("Error closing {}", name, e); }
    }

    private void safeDisconnectSource() {
        try { source.disconnect(); } catch (Exception ignored) {}
    }

    private static String normalizeNonBlank(String s, String fallback) {
        if (s == null) return fallback;
        String t = s.trim();
        return t.isEmpty() ? fallback : t;
    }
}


