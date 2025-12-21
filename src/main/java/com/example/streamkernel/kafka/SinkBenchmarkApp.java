package com.example.streamkernel.kafka;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.config.PipelineFactory;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.metrics.MetricsFactory;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.LongAdder;

/**
 * Standalone tool to stress-test JUST the Sink (Kafka/Mongo/DevNull).
 * Bypasses the Orchestrator to find the maximum write speed of the infrastructure.
 */
public class SinkBenchmarkApp {

    private static final Logger log = LoggerFactory.getLogger(SinkBenchmarkApp.class);

    public static void main(String[] args) {
        log.info("=== Sink Benchmark Tool (Infrastructure Stress Test) ===");

        PipelineConfig config = PipelineConfig.get();
        MetricsRuntime metrics = MetricsFactory.init(config);

        // Load config
        int payloadSize = Integer.parseInt(config.getProperty("benchmark.payload.bytes", "1024")); // 1KB default
        int targetCount = Integer.parseInt(config.getProperty("benchmark.target.count", "1000000")); // 1M items

        log.info("🔥 Benchmark Config: Payload={} bytes | Target={} items", payloadSize, targetCount);

        // Generate static payload once to save CPU
        String staticData = "X".repeat(payloadSize);
        PipelinePayload<String> payload = new PipelinePayload<>(
                "BENCH-1",
                staticData,
                Instant.now(),
                Collections.emptyMap()
        );

        OutputSink<String> actualSink = null;

        try {
            // FIX: Explicitly cast the generic sink to String for this benchmark
            // We know we are sending Strings (staticData), so this is safe.
            @SuppressWarnings("unchecked")
            OutputSink<String> castSink = (OutputSink<String>) PipelineFactory.createSink(config, metrics);
            actualSink = castSink;

            log.info("✅ Connected to Sink: {}", actualSink.getClass().getSimpleName());
            log.info("🚀 Starting load test...");

            long start = System.currentTimeMillis();
            LongAdder counter = new LongAdder();

            // Hot loop
            for (int i = 0; i < targetCount; i++) {
                try {
                    actualSink.write(payload);
                    counter.increment();
                } catch (Exception e) {
                    log.error("Write failed", e);
                    break;
                }

                // Progress Report every 100k
                if (i % 100_000 == 0 && i > 0) {
                    printSpeed(start, i);
                }
            }

            long end = System.currentTimeMillis();
            long total = counter.sum();
            double seconds = (end - start) / 1000.0;
            double eps = total / seconds;

            log.info("🏁 FINISHED: Sent {} items in {}s | Speed: {},.0f EPS", total, String.format("%.2f", seconds), eps);

        } catch (Exception e) {
            log.error("Benchmark failed", e);
        } finally {
            if (actualSink != null) {
                try { actualSink.close(); } catch (Exception ignored) {}
            }
            metrics.close();
        }
    }

    private static void printSpeed(long startMs, long count) {
        long now = System.currentTimeMillis();
        double seconds = (now - startMs) / 1000.0;
        if (seconds > 0) {
            double eps = count / seconds;
            System.out.printf("   ... %d items | %,.0f EPS%n", count, eps);
        }
    }
}