package com.example.streamkernel.kafka.core;

import com.example.streamkernel.kafka.security.OpaAuthorizer;
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
 * ENTERPRISE ORCHESTRATOR (v5.2 - Best Practice: OPA + Performance + No Feature Loss)
 *
 * Preserves:
 *  - Backpressure-first dispatch (permit acquired before fetch)
 *  - Virtual-thread-per-batch execution
 *  - Cache stores whole PipelinePayload by id
 *  - DLQ on failures and on denial (original payload)
 *  - Bounded drain on stop
 *
 * Best-practice additions:
 *  - Per-batch OPA auth with short TTL cache (default)
 *  - Local chunking if source returns oversized batches (enforces batchSize contract)
 *  - Fail-closed OPA with operational visibility (authErrors counter + rate-limited ERROR log)
 *  - Avoid log storms (one WARN per denied batch chunk)
 */
public class PipelineOrchestrator<I, O> {

    private static final Logger log = LoggerFactory.getLogger(PipelineOrchestrator.class);

    // Core Components
    private final SourceConnector<I> source;
    private final OutputSink<O> primarySink;
    private final OutputSink<I> dlqSink;
    private final Transformer<I, O> transformer;
    private final CacheStrategy<I> cache;
    private final LongAdder meter; // optional

    // Security Components
    private final OpaAuthorizer opaAuthorizer; // nullable
    private final String serviceAccount;
    private final String targetResource;
    private final String action;

    // Tuning
    private final int parallelism;
    private final int batchSize;

    // Authorization tuning
    private final boolean authorizePerRecord; // default false for performance
    private final long authTtlNanos;           // cache TTL; 0 disables caching

    // Source error policy
    private final boolean failFastOnSourceError;
    private final long sourceErrorBackoffInitialMs;
    private final long sourceErrorBackoffMaxMs;

    // Threading / Lifecycle
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;
    private Thread dispatcherThread;
    private Semaphore concurrencyLimiter;

    private final AtomicInteger inFlightBatches = new AtomicInteger(0);
    private final Object drainMonitor = new Object();

    // Counters
    private final LongAdder deniedCount = new LongAdder();
    private final LongAdder dlqCount = new LongAdder();
    private final LongAdder sourceErrorCount = new LongAdder();
    private final LongAdder authErrorCount = new LongAdder();

    // Auth decision cache (single-key cache: serviceAccount+action+targetResource stable per orchestrator)
    private volatile long authCacheExpiresAtNanos = 0L;
    private volatile Boolean authCacheValue = null;

    // Rate-limit auth error logging to avoid storms
    private volatile long lastAuthErrorLogAtMs = 0L;

    public PipelineOrchestrator(SourceConnector<I> source,
                                OutputSink<O> primarySink,
                                OutputSink<I> dlqSink,
                                Transformer<I, O> transformer,
                                CacheStrategy<I> cache,
                                LongAdder meter,
                                int parallelism,
                                int batchSize,
                                OpaAuthorizer opaAuthorizer,
                                String serviceAccount,
                                String targetResource) {

        this(source, primarySink, dlqSink, transformer, cache, meter,
                parallelism, batchSize,
                opaAuthorizer,
                serviceAccount, targetResource,
                "write",
                false,                      // authorizePerRecord
                TimeUnit.SECONDS.toNanos(1),// auth TTL
                false,                      // failFastOnSourceError
                250L,                       // initial backoff
                5_000L                      // max backoff
        );
    }

    public PipelineOrchestrator(SourceConnector<I> source,
                                OutputSink<O> primarySink,
                                OutputSink<I> dlqSink,
                                Transformer<I, O> transformer,
                                CacheStrategy<I> cache,
                                LongAdder meter,
                                int parallelism,
                                int batchSize,
                                OpaAuthorizer opaAuthorizer,
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
        this.meter = meter;

        this.opaAuthorizer = opaAuthorizer;
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

        source.connect();

        // Throughput-optimized semaphore (non-fair). Use fair only if you have proven starvation.
        this.concurrencyLimiter = new Semaphore(parallelism);

        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        this.dispatcherThread = new Thread(this::runDispatcherLoop, "pipeline-dispatcher");
        this.dispatcherThread.setDaemon(true);
        this.dispatcherThread.start();

        log.info("Pipeline Started: parallelism={} batchSize={} opaEnabled={} user={} resource={} action={} authMode={} authTtlMs={} failFastSourceError={}",
                parallelism,
                batchSize,
                (opaAuthorizer != null),
                serviceAccount,
                targetResource,
                action,
                authorizePerRecord ? "per-record" : "per-batch",
                TimeUnit.NANOSECONDS.toMillis(authTtlNanos),
                failFastOnSourceError
        );
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        log.info("Stop requested. Draining in-flight batches...");

        if (dispatcherThread != null) dispatcherThread.interrupt();

        long deadline = System.currentTimeMillis() + 10_000L;
        synchronized (drainMonitor) {
            while (inFlightBatches.get() > 0 && System.currentTimeMillis() < deadline) {
                try {
                    drainMonitor.wait(250L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        safeClose(primarySink, "primarySink");
        safeClose(dlqSink, "dlqSink");
        safeDisconnectSource();

        log.info("Pipeline stopped. remainingInFlight={} denied={} dlq={} sourceErrors={} authErrors={}",
                inFlightBatches.get(),
                deniedCount.sum(),
                dlqCount.sum(),
                sourceErrorCount.sum(),
                authErrorCount.sum()
        );
    }

    private void safeClose(Object sink, String name) {
        try {
            if (sink instanceof AutoCloseable c) c.close();
        } catch (Exception e) {
            log.warn("Failed to close {}", name, e);
        }
    }

    private void safeDisconnectSource() {
        try {
            source.disconnect();
        } catch (Exception e) {
            log.warn("Failed to disconnect source", e);
        }
    }

    private void runDispatcherLoop() {
        int idleMs = 1;
        long sourceErrorBackoffMs = sourceErrorBackoffInitialMs;

        try {
            while (running.get()) {

                // Backpressure FIRST
                concurrencyLimiter.acquire();

                List<PipelinePayload<I>> batch;
                try {
                    batch = source.fetchBatch(batchSize);
                    sourceErrorBackoffMs = sourceErrorBackoffInitialMs;
                } catch (Exception e) {
                    concurrencyLimiter.release();
                    sourceErrorCount.increment();

                    if (failFastOnSourceError) {
                        log.error("Source fetch failed (fail-fast enabled). Stopping dispatcher.", e);
                        running.set(false);
                        break;
                    }

                    log.error("Source fetch failed (will retry).", e);

                    long sleepMs = (sourceErrorBackoffMs <= 0) ? 1000L : sourceErrorBackoffMs;
                    sleepMs = Math.min(sleepMs, sourceErrorBackoffMaxMs);

                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    if (sourceErrorBackoffMs > 0) {
                        sourceErrorBackoffMs = Math.min(sourceErrorBackoffMs * 2, sourceErrorBackoffMaxMs);
                    }
                    continue;
                }

                if (batch == null || batch.isEmpty()) {
                    concurrencyLimiter.release();
                    try {
                        Thread.sleep(idleMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    idleMs = Math.min(idleMs * 2, 50);
                    continue;
                }

                idleMs = 1;
                inFlightBatches.incrementAndGet();

                executor.submit(() -> {
                    try {
                        processPossiblyOversizedBatch(batch);
                    } finally {
                        concurrencyLimiter.release();
                        if (meter != null) meter.add(batch.size());

                        int remaining = inFlightBatches.decrementAndGet();
                        if (remaining == 0) {
                            synchronized (drainMonitor) {
                                drainMonitor.notifyAll();
                            }
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

    private void processPossiblyOversizedBatch(List<PipelinePayload<I>> batch) {
        final int n = batch.size();
        if (n <= batchSize) {
            processBatch(batch);
            return;
        }

        // Enforce batchSize contract even if source overproduces.
        for (int start = 0; start < n; start += batchSize) {
            int end = Math.min(start + batchSize, n);
            processBatch(batch.subList(start, end));
        }
    }

    private void processBatch(List<PipelinePayload<I>> batch) {
        final var sink = this.primarySink;
        final var trans = this.transformer;
        final var cacheStrategy = this.cache;
        final var opa = this.opaAuthorizer;

        // Per-batch auth (default best practice)
        if (opa != null && !authorizePerRecord) {
            boolean allowed = isAllowedCachedFailClosed(opa);
            if (!allowed) {
                deniedCount.add(batch.size());
                for (PipelinePayload<I> payload : batch) {
                    safeCachePut(cacheStrategy, payload);
                    safeDlqWrite(payload);
                }
                log.warn("SECURITY DENIAL (batch): user='{}' action='{}' resource='{}' batchSize={}",
                        serviceAccount, action, targetResource, batch.size());
                return;
            }
        }

        for (PipelinePayload<I> payload : batch) {
            try {
                // 1) Cache raw input (audit trail)
                safeCachePut(cacheStrategy, payload);

                // 2) Optional per-record auth (only if explicitly enabled)
                if (opa != null && authorizePerRecord) {
                    boolean allowed = isAllowedFailClosed(opa);
                    if (!allowed) {
                        deniedCount.increment();
                        safeDlqWrite(payload);
                        continue;
                    }
                }

                // 3) Transform
                PipelinePayload<O> result = trans.transform(payload);

                // 4) Write
                sink.write(result);

            } catch (Exception e) {
                handleError(payload, e);
            }
        }
    }

    private boolean isAllowedCachedFailClosed(OpaAuthorizer opa) {
        if (authTtlNanos <= 0) return isAllowedFailClosed(opa);

        long now = System.nanoTime();
        Boolean cached = authCacheValue;
        if (cached != null && now < authCacheExpiresAtNanos) return cached;

        boolean allowed = isAllowedFailClosed(opa);
        authCacheValue = allowed;
        authCacheExpiresAtNanos = now + authTtlNanos;
        return allowed;
    }

    private boolean isAllowedFailClosed(OpaAuthorizer opa) {
        try {
            return opa.isAllowed(serviceAccount, action, targetResource);
        } catch (Exception e) {
            authErrorCount.increment();

            long nowMs = System.currentTimeMillis();
            if (nowMs - lastAuthErrorLogAtMs > 5_000L) {
                lastAuthErrorLogAtMs = nowMs;
                log.error("OPA authorization error (fail-closed). user='{}' action='{}' resource='{}'. Denying until healthy.",
                        serviceAccount, action, targetResource, e);
            }
            return false;
        }
    }

    private void safeCachePut(CacheStrategy<I> cacheStrategy, PipelinePayload<I> payload) {
        try {
            cacheStrategy.put(payload.id(), payload);
        } catch (Exception e) {
            // Cache failures should not break the pipeline
            log.debug("Cache put failed. id={} msg={}", payload.id(), e.getMessage());
        }
    }

    private void safeDlqWrite(PipelinePayload<I> payload) {
        try {
            dlqSink.write(payload);
            dlqCount.increment();
        } catch (Exception dlqError) {
            log.error("DOUBLE FAULT: Failed to write to DLQ. id={}", payload.id(), dlqError);
        }
    }

    private void handleError(PipelinePayload<I> payload, Exception e) {
        log.warn("Record failed. id={} type={} msg={}",
                payload.id(), e.getClass().getSimpleName(), e.getMessage());
        log.debug("Record failure details. id={}", payload.id(), e);
        safeDlqWrite(payload);
    }
}
