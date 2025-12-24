package com.example.streamkernel.kafka;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.config.PipelineFactory;
import com.example.streamkernel.kafka.core.*;
import com.example.streamkernel.kafka.metrics.MetricsFactory;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.output.KafkaSink;
import com.example.streamkernel.kafka.security.OpaAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

public class KafkaApp {

    private static final Logger log = LoggerFactory.getLogger(KafkaApp.class);

    public static void main(String[] args) {
        log.info("=== Booting StreamKernel (v4.0 - Generics + Security + Speedometer) ===");

        PipelineConfig config = PipelineConfig.get();
        PipelineFactory.logAvailablePlugins();
        MetricsRuntime metrics = MetricsFactory.init(config);

        int parallelism = Integer.parseInt(config.getProperty("pipeline.parallelism", "50"));
        int appBatchSize = Integer.parseInt(config.getProperty("pipeline.app.batch.size", "4000"));

        log.info("🔧 CONFIG: Parallelism={} | AppBatchSize={}", parallelism, appBatchSize);

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        // We use arrays to hold references for the shutdown hook lambda
        final OutputSink<?>[] sinks = new OutputSink<?>[2];

        try {
            // 1. Create Components (Generic Types)
            SourceConnector<?> source = PipelineFactory.createSource(config, metrics);
            OutputSink<?> sink = PipelineFactory.createSink(config, metrics);
            OutputSink<?> dlq = PipelineFactory.createDlq(config, metrics);
            Transformer<?, ?> transformer = PipelineFactory.createTransformer(config, metrics);
            CacheStrategy<?> cache = PipelineFactory.createCache(config, metrics);

            sinks[0] = sink;
            sinks[1] = dlq;

            LongAdder processedCounter = new LongAdder();

            // 2. Security Setup (New)
            String opaUrl = config.getProperty("security.opa.url"); // e.g. http://localhost:8181...
            OpaAuthorizer opa = (opaUrl != null && !opaUrl.isBlank()) ? new OpaAuthorizer(opaUrl) : null;
            String serviceAccount = config.getProperty("app.service.account", "default-service-account");
            String targetResource = config.getProperty("sink.topic", "unknown-topic");

            // 3. Instantiate Orchestrator using Raw Types
            // @SuppressWarnings lets us wire up dynamic plugins (Avro/String/Mongo) safely
            @SuppressWarnings({"rawtypes", "unchecked"})
            PipelineOrchestrator pipeline = new PipelineOrchestrator(
                    source,
                    sink,
                    dlq,
                    transformer,
                    cache,
                    processedCounter,
                    parallelism,
                    appBatchSize,
                    opa,               // <--- NEW: OPA Authorizer
                    serviceAccount,    // <--- NEW: Identity
                    targetResource     // <--- NEW: Resource
            );

            // 4. Speedometer Setup (Retaining your logic)
            KafkaSink kafkaSink = (sink instanceof KafkaSink ks) ? ks : null;

            int windowSeconds = Integer.parseInt(config.getProperty("streamkernel.speedometer.window.seconds", "10"));
            windowSeconds = Math.max(5, Math.min(60, windowSeconds));

            startSpeedometer(processedCounter, kafkaSink, windowSeconds);

            // 5. Shutdown Hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("🛑 Shutdown signal received...");
                try {
                    pipeline.stop();
                } finally {
                    try { if (sinks[0] != null) sinks[0].close(); } catch (Exception ignored) {}
                    try { if (sinks[1] != null) sinks[1].close(); } catch (Exception ignored) {}
                    try { metrics.close(); } catch (Exception ignored) {}
                    shutdownLatch.countDown();
                }
            }));

            pipeline.start();
            shutdownLatch.await();

        } catch (Exception e) {
            log.error("Fatal Application Error", e);
            try { if (sinks[0] != null) sinks[0].close(); } catch (Exception ignored) {}
            try { if (sinks[1] != null) sinks[1].close(); } catch (Exception ignored) {}
            try { metrics.close(); } catch (Exception ignored) {}
            System.exit(1);
        }
    }

    // --- YOUR ORIGINAL SPEEDOMETER LOGIC (Restored) ---
    private static void startSpeedometer(LongAdder processedCounter, KafkaSink kafkaSink, int windowSeconds) {
        Thread speedThread = new Thread(() -> {
            long lastProcessed = 0;
            long lastAcked = 0;
            long lastFailed = 0;

            long windowStartMs = System.currentTimeMillis();

            if (kafkaSink != null) {
                log.info("🏎️  Speedometer Active ({}s window): Processed + Acked + Fail + InFlight", windowSeconds);
            } else {
                log.info("🏎️  Speedometer Active ({}s window): Processed only", windowSeconds);
            }

            while (!Thread.currentThread().isInterrupted()) {
                try { Thread.sleep(1000); } catch (InterruptedException e) { break; }

                long nowMs = System.currentTimeMillis();
                long elapsedMs = nowMs - windowStartMs;

                // Wait for the full window (e.g., 10s) before printing
                if (elapsedMs < (windowSeconds * 1000L)) {
                    continue;
                }

                double seconds = elapsedMs / 1000.0;

                long processedNow = processedCounter.sum();
                double processedEps = (processedNow - lastProcessed) / seconds;

                if (kafkaSink != null) {
                    long ackedNow = kafkaSink.sentOkTotal();
                    long failedNow = kafkaSink.sentFailTotal();
                    long inFlightNow = kafkaSink.inFlightTotal();

                    double ackedEps = (ackedNow - lastAcked) / seconds;
                    double failEps  = (failedNow - lastFailed) / seconds;

                    System.out.printf(
                            "🔥 %ds AVG | SPEED: %,.0f eps | ✅ ACKED: %,.0f eps | ❌ FAIL: %,.0f eps | ⏳ INFLIGHT: %,.0f%n",
                            windowSeconds,
                            processedEps,
                            ackedEps,
                            failEps,
                            (double) inFlightNow
                    );

                    lastAcked = ackedNow;
                    lastFailed = failedNow;
                } else {
                    System.out.printf("🔥 %ds AVG | SPEED: %,.0f Events/Sec%n", windowSeconds, processedEps);
                }

                lastProcessed = processedNow;
                windowStartMs = nowMs;
            }
        });

        speedThread.setDaemon(true);
        speedThread.start();
    }
}