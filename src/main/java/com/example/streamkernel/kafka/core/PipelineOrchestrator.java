package com.example.streamkernel.kafka.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * ENTERPRISE ORCHESTRATOR (v4.1 - Compilation Fix)
 * - Fixed Type Mismatch in CacheStrategy.
 * - Stores full PipelinePayload in Cache.
 */
public class PipelineOrchestrator<I, O> {

    private static final Logger log = LoggerFactory.getLogger(PipelineOrchestrator.class);

    private final SourceConnector<I> source;
    private final OutputSink<O> primarySink;
    private final OutputSink<I> dlqSink;
    private final Transformer<I, O> transformer;
    private final CacheStrategy<I> cache;
    private final LongAdder meter;

    private final int parallelism;
    private final int batchSize;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;
    private Thread dispatcherThread;
    private Semaphore concurrencyLimiter;

    private final java.util.concurrent.atomic.AtomicInteger inFlightBatches = new java.util.concurrent.atomic.AtomicInteger(0);
    private final Object drainMonitor = new Object();

    public PipelineOrchestrator(SourceConnector<I> source,
                                OutputSink<O> primarySink,
                                OutputSink<I> dlqSink,
                                Transformer<I, O> transformer,
                                CacheStrategy<I> cache,
                                LongAdder meter,
                                int parallelism,
                                int batchSize) {

        this.source = Objects.requireNonNull(source, "source");
        this.primarySink = Objects.requireNonNull(primarySink, "primarySink");
        this.dlqSink = Objects.requireNonNull(dlqSink, "dlqSink");
        this.transformer = Objects.requireNonNull(transformer, "transformer");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.meter = meter;

        if (parallelism <= 0) throw new IllegalArgumentException("Parallelism must be > 0");
        if (batchSize <= 0) throw new IllegalArgumentException("BatchSize must be > 0");

        this.parallelism = parallelism;
        this.batchSize = batchSize;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;

        source.connect();
        this.concurrencyLimiter = new Semaphore(parallelism);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        this.dispatcherThread = new Thread(this::runDispatcherLoop, "pipeline-dispatcher");
        this.dispatcherThread.setDaemon(true);
        this.dispatcherThread.start();

        log.info("🚀 Pipeline Started: Parallelism={} | BatchSize={}", parallelism, batchSize);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        log.info("🛑 Stop requested. Draining in-flight batches...");

        // Stop dispatcher loop quickly
        if (dispatcherThread != null) dispatcherThread.interrupt();

        // Wait for in-flight batches (bounded)
        long deadline = System.currentTimeMillis() + 10_000L;
        synchronized (drainMonitor) {
            while (inFlightBatches.get() > 0 && System.currentTimeMillis() < deadline) {
                try { drainMonitor.wait(250L); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
        }

        // Now shutdown executor cleanly
        if (executor != null) {
            executor.shutdown();
            try { executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        // Close in correct order (best effort)
        safeClose(primarySink, "primarySink");
        safeClose(dlqSink, "dlqSink");
        safeDisconnectSource();

        log.info("✅ Pipeline stopped. Remaining in-flight={}", inFlightBatches.get());
    }

    private void safeClose(Object sink, String name) {
        try {
            if (sink instanceof AutoCloseable c) c.close();
        } catch (Exception e) {
            log.warn("Failed to close {}", name, e);
        }
    }

    private void safeDisconnectSource() {
        try { source.disconnect(); }
        catch (Exception e) { log.warn("Failed to disconnect source", e); }
    }


    private void runDispatcherLoop() {
        int idleMs = 1;

        try {
            while (running.get()) {

                // Backpressure FIRST: do not fetch if we cannot dispatch
                concurrencyLimiter.acquire();

                List<PipelinePayload<I>> batch;
                try {
                    batch = source.fetchBatch(batchSize);
                } catch (Exception e) {
                    concurrencyLimiter.release();
                    throw e;
                }

                if (batch == null || batch.isEmpty()) {
                    concurrencyLimiter.release();

                    // adaptive backoff (caps at 50ms)
                    try { Thread.sleep(idleMs); } catch (InterruptedException ie) { break; }
                    idleMs = Math.min(idleMs * 2, 50);
                    continue;
                }

                idleMs = 1;

                inFlightBatches.incrementAndGet();

                executor.submit(() -> {
                    try {
                        processBatch(batch);
                    } finally {
                        concurrencyLimiter.release();
                        if (meter != null) meter.add(batch.size());

                        int remaining = inFlightBatches.decrementAndGet();
                        if (remaining == 0) {
                            synchronized (drainMonitor) { drainMonitor.notifyAll(); }
                        }
                    }
                });
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("Dispatcher stopping...");
        } catch (Exception e) {
            log.error("Dispatcher crashed", e);
            running.set(false);
        }
    }

    private void processBatch(List<PipelinePayload<I>> batch) {
        var sink = this.primarySink;
        var trans = this.transformer;
        var cacheStrategy = this.cache;

        for (PipelinePayload<I> payload : batch) {
            try {
                // FIXED: Pass the WHOLE payload object, not just .data()
                cacheStrategy.put(payload.id(), payload);

                sink.write(trans.transform(payload));

            } catch (Exception e) {
                handleError(payload, e);
            }
        }
    }

    private void handleError(PipelinePayload<I> payload, Exception e) {
        log.warn("Record failed. id={}", payload.id(), e);
        try {
            dlqSink.write(payload);
        } catch (Exception dlqError) {
            log.error("DOUBLE FAULT: Failed to write to DLQ. id={}", payload.id(), dlqError);
        }
    }
}