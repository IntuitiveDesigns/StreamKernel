/*
 * Copyright 2025 Steven Lopez
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.kafka;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.config.PipelineFactory;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelineOrchestrator;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.output.KafkaSink;
import com.intuitivedesigns.streamkernel.metrics.MetricsFactory;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.metrics.MetricsSettings;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider; // NEW IMPORT
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public final class KafkaApp {

    private static final Logger log = LoggerFactory.getLogger(KafkaApp.class);

    // --- Config Keys ---
    private static final String CFG_PARALLELISM = "pipeline.parallelism";
    private static final String CFG_PIPELINE_BATCH_SIZE = "pipeline.batch.size";
    private static final String CFG_SPEEDOMETER_WINDOW_SECONDS = "streamkernel.speedometer.window.seconds";
    private static final String CFG_SPEEDOMETER_ENABLED = "streamkernel.speedometer.enabled";

    // Security Configs
    private static final String CFG_SERVICE_ACCOUNT = "app.service.account";
    private static final String CFG_TARGET_RESOURCE = "security.resource"; // often defaults to sink.topic
    private static final String CFG_ACTION = "security.action";
    private static final String CFG_AUTH_PER_RECORD = "security.auth.per.record";
    private static final String CFG_AUTH_TTL_MS = "security.auth.ttl.ms";
    private static final String CFG_SOURCE_FAIL_FAST = "pipeline.source.fail.fast";

    // --- Defaults ---
    private static final int DEFAULT_PARALLELISM = 50;
    private static final int DEFAULT_PIPELINE_BATCH_SIZE = 100;
    private static final int DEFAULT_WINDOW_SECONDS = 10;
    private static final int MIN_WINDOW_SECONDS = 5;
    private static final int MAX_WINDOW_SECONDS = 60;

    private KafkaApp() {}

    public static void main(String[] args) {
        log.info("=== Booting StreamKernel (v6.0 - SPI Security + Generics + Speedometer) ===");

        final PipelineConfig config = PipelineConfig.get();
        PipelineFactory.logAvailablePlugins();

        MetricsRuntime metrics = null;
        ScheduledExecutorService speedometerScheduler = null;

        SourceConnector<?> source = null;
        OutputSink<?> sink = null;
        OutputSink<?> dlq = null;
        Transformer<?, ?> transformer = null;
        Cache<?, ?> cache = null;
        // NEW: Security Provider reference for cleanup if needed (though usually stateless)
        SecurityProvider securityProvider;

        PipelineOrchestrator pipeline = null;

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        final AtomicBoolean shutdownStarted = new AtomicBoolean(false);

        try {
            // 1. Initialize Metrics
            metrics = MetricsFactory.init(MetricsSettings.from(config));

            // 2. Load Tuning Configs
            final int parallelism = clampInt(config.getInt(CFG_PARALLELISM, DEFAULT_PARALLELISM), 1, Integer.MAX_VALUE);
            final int pipelineBatchSize = clampInt(config.getInt(CFG_PIPELINE_BATCH_SIZE, DEFAULT_PIPELINE_BATCH_SIZE), 1, Integer.MAX_VALUE);

            final boolean speedometerEnabled = config.getBoolean(CFG_SPEEDOMETER_ENABLED, true);
            final int windowSeconds = clampInt(
                    config.getInt(CFG_SPEEDOMETER_WINDOW_SECONDS, DEFAULT_WINDOW_SECONDS),
                    MIN_WINDOW_SECONDS,
                    MAX_WINDOW_SECONDS
            );

            // 3. Load Security Context & Tuning
            final String serviceAccount = config.getString(CFG_SERVICE_ACCOUNT, "unknown-service");
            // Default resource to the topic name if specific security resource not set
            final String targetResource = config.getString(CFG_TARGET_RESOURCE, config.getString("sink.topic", "unknown-resource"));
            final String action = config.getString(CFG_ACTION, "write");

            final boolean authPerRecord = config.getBoolean(CFG_AUTH_PER_RECORD, false);
            final long authTtlMs = Long.parseLong(config.getString(CFG_AUTH_TTL_MS, "1000"));
            final boolean failFastSource = config.getBoolean(CFG_SOURCE_FAIL_FAST, false);

            log.info("CONFIG: Parallelism={} | Batch={} | Speedometer={} | Security=[User:{} Resource:{} Action:{} Mode:{}]",
                    parallelism, pipelineBatchSize, speedometerEnabled ? "ON" : "OFF",
                    serviceAccount, targetResource, action, authPerRecord ? "Per-Record" : "Per-Batch");

            // 4. Create Components (SPI)
            source = PipelineFactory.createSource(config, metrics);
            sink = PipelineFactory.createSink(config, metrics);
            dlq = PipelineFactory.createDlq(config, metrics);
            transformer = PipelineFactory.createTransformer(config, metrics);
            cache = PipelineFactory.createCache(config, metrics);

            // NEW: Load Security Provider via SPI
            securityProvider = PipelineFactory.createSecurity(config, metrics);

            final LongAdder processedCounter = new LongAdder();

            // 5. Instantiate Orchestrator
            // We use raw types here because the Orchestrator internally manages type safety between components
            @SuppressWarnings({"rawtypes", "unchecked"})
            PipelineOrchestrator createdPipeline = new PipelineOrchestrator(
                    source,
                    sink,
                    dlq,
                    transformer,
                    cache,
                    processedCounter,
                    parallelism,
                    pipelineBatchSize,
                    securityProvider,       // <--- Injected Provider
                    serviceAccount,
                    targetResource,
                    action,
                    authPerRecord,
                    TimeUnit.MILLISECONDS.toNanos(authTtlMs),
                    failFastSource,
                    250L,   // Initial backoff (could be config)
                    5000L   // Max backoff (could be config)
            );
            pipeline = createdPipeline;

            // 6. Setup Speedometer
            final KafkaSink kafkaSink = (sink instanceof KafkaSink ks) ? ks : null;

            if (speedometerEnabled) {
                speedometerScheduler = Executors.newSingleThreadScheduledExecutor(new NamedDaemonThreadFactory("sk-speedometer"));
                startSpeedometer(speedometerScheduler, processedCounter, kafkaSink, windowSeconds);
            }

            // 7. Setup Shutdown Hook
            final MetricsRuntime finalMetrics = metrics;
            final ScheduledExecutorService finalSpeedometerScheduler = speedometerScheduler;
            final SourceConnector<?> finalSource = source;
            final OutputSink<?> finalSink = sink;
            final OutputSink<?> finalDlq = dlq;
            final PipelineOrchestrator finalPipeline = pipeline;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!shutdownStarted.compareAndSet(false, true)) {
                    return;
                }

                log.info("Shutdown signal received.");
                try {
                    if (finalSpeedometerScheduler != null) {
                        finalSpeedometerScheduler.shutdownNow();
                    }
                    if (finalPipeline != null) {
                        finalPipeline.stop();
                    }
                } finally {
                    closeQuietly(finalSource);
                    closeQuietly(finalSink);
                    closeQuietly(finalDlq);
                    closeQuietly(finalMetrics);
                    shutdownLatch.countDown();
                }
            }, "sk-shutdown"));

            // 8. Launch
            log.info("Starting Pipeline Orchestrator...");
            pipeline.start();

            shutdownLatch.await();
        } catch (Throwable t) {
            log.error("Fatal application error", t);

            if (shutdownStarted.compareAndSet(false, true)) {
                if (speedometerScheduler != null) {
                    speedometerScheduler.shutdownNow();
                }
                if (pipeline != null) {
                    try { pipeline.stop(); } catch (Exception ignored) {}
                }
                closeQuietly(source);
                closeQuietly(sink);
                closeQuietly(dlq);
                closeQuietly(metrics);
                shutdownLatch.countDown();
            }

            System.exit(1);
        }
    }

    private static void startSpeedometer(
            ScheduledExecutorService scheduler,
            LongAdder processedCounter,
            KafkaSink kafkaSink,
            int windowSeconds
    ) {
        log.info("Speedometer active ({}s window){}", windowSeconds, (kafkaSink != null ? ": Processed + Acked + Fail + InFlight" : ": Processed only"));

        final long periodNs = TimeUnit.SECONDS.toNanos(windowSeconds);

        scheduler.scheduleAtFixedRate(new Runnable() {
            private long lastTimeNs = System.nanoTime();
            private long lastProcessed = 0;
            private long lastAcked = 0;
            private long lastFailed = 0;

            @Override
            public void run() {
                try {
                    final long nowNs = System.nanoTime();
                    final long elapsedNs = nowNs - lastTimeNs;

                    if (elapsedNs <= 0) {
                        return;
                    }

                    final double seconds = elapsedNs / 1_000_000_000.0;

                    final long processedNow = processedCounter.sum();
                    final double processedEps = (processedNow - lastProcessed) / seconds;

                    if (kafkaSink != null) {
                        final long ackedNow = kafkaSink.sentOkTotal();
                        final long failedNow = kafkaSink.sentFailTotal();
                        final long inFlightNow = kafkaSink.inFlightTotal();

                        final double ackedEps = (ackedNow - lastAcked) / seconds;
                        final double failEps = (failedNow - lastFailed) / seconds;

                        log.info(String.format(
                                Locale.US,
                                "AVG %ds | SPEED: %,.0f eps | ACKED: %,.0f eps | FAIL: %,.0f eps | INFLIGHT: %,d",
                                windowSeconds,
                                processedEps,
                                ackedEps,
                                failEps,
                                inFlightNow
                        ));

                        lastAcked = ackedNow;
                        lastFailed = failedNow;
                    } else {
                        log.info(String.format(
                                Locale.US,
                                "AVG %ds | SPEED: %,.0f eps",
                                windowSeconds,
                                processedEps
                        ));
                    }

                    lastProcessed = processedNow;
                    lastTimeNs = nowNs;
                } catch (Throwable t) {
                    log.warn("Speedometer error", t);
                }
            }
        }, periodNs, periodNs, TimeUnit.NANOSECONDS);
    }

    private static void closeQuietly(Object resource) {
        if (resource == null) return;
        if (resource instanceof AutoCloseable c) {
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static final class NamedDaemonThreadFactory implements ThreadFactory {
        private final String name;

        private NamedDaemonThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        }
    }
}


