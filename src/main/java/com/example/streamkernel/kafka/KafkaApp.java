package com.example.streamkernel.kafka;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.config.PipelineFactory;
import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.PipelineOrchestrator;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.core.Transformer;
import com.example.streamkernel.kafka.metrics.MetricsFactory;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.output.KafkaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

public class KafkaApp {

    private static final Logger log = LoggerFactory.getLogger(KafkaApp.class);

    public static void main(String[] args) {
        log.info("=== Booting Enterprise Pipeline Agent (v3.4 - 10s Acked EPS + InFlight) ===");

        PipelineConfig config = PipelineConfig.get();
        PipelineFactory.logAvailablePlugins();
        MetricsRuntime metrics = MetricsFactory.init(config);

        int parallelism = Integer.parseInt(config.getProperty("pipeline.parallelism", "50"));
        int appBatchSize = Integer.parseInt(config.getProperty("pipeline.app.batch.size", "4000"));

        log.info("🔧 CONFIG: Parallelism={} | AppBatchSize={}", parallelism, appBatchSize);

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        OutputSink<String> sink = null;
        OutputSink<String> dlq  = null;

        try {
            SourceConnector<String> source = PipelineFactory.createSource(config, metrics);
            sink = PipelineFactory.createSink(config, metrics);
            dlq  = PipelineFactory.createDlq(config, metrics);
            Transformer<String, String> transformer = PipelineFactory.createTransformer(config, metrics);
            CacheStrategy<String> cache = PipelineFactory.createCache(config, metrics);

            LongAdder processedCounter = new LongAdder();

            PipelineOrchestrator<String, String> pipeline = new PipelineOrchestrator<>(
                    source,
                    sink,
                    dlq,
                    transformer,
                    cache,
                    processedCounter,
                    parallelism,
                    appBatchSize
            );

            KafkaSink kafkaSink = (sink instanceof KafkaSink ks) ? ks : null;

            // 10-second stable window
            int windowSeconds = Integer.parseInt(config.getProperty("streamkernel.speedometer.window.seconds", "10"));
            windowSeconds = Math.max(5, Math.min(60, windowSeconds));

            startSpeedometer(processedCounter, kafkaSink, windowSeconds);

            OutputSink<String> finalSink = sink;
            OutputSink<String> finalDlq  = dlq;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("🛑 Shutdown signal received...");
                try {
                    pipeline.stop();
                } finally {
                    try { if (finalSink != null) finalSink.close(); } catch (Exception ignored) {}
                    try { if (finalDlq  != null) finalDlq.close(); } catch (Exception ignored) {}
                    try { metrics.close(); } catch (Exception ignored) {}
                    shutdownLatch.countDown();
                }
            }));

            pipeline.start();
            shutdownLatch.await();

        } catch (Exception e) {
            log.error("Fatal Application Error", e);
            try { if (sink != null) sink.close(); } catch (Exception ignored) {}
            try { if (dlq  != null) dlq.close(); } catch (Exception ignored) {}
            try { metrics.close(); } catch (Exception ignored) {}
            System.exit(1);
        }
    }

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

                if (elapsedMs < (windowSeconds * 1000L)) {
                    continue; // wait until full window completes
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
