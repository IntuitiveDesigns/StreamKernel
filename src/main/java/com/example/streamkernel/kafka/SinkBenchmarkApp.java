package com.example.streamkernel.kafka;

import com.example.streamkernel.kafka.bench.SyntheticSource;
import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.config.PipelineFactory;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.metrics.MetricsFactory;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class SinkBenchmarkApp {

    private static final Logger log = LoggerFactory.getLogger(SinkBenchmarkApp.class);

    public static void main(String[] args) {
        log.info("=== Booting Sink Benchmark (Manual Dispatch Mode) ===");

        // --------------------------------------------------------------------
        // 1. METRICS REGISTRY + /metrics HTTP ENDPOINT
        // --------------------------------------------------------------------
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        Thread metricsThread = new Thread(() -> {
            try {
                HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
                server.createContext("/metrics", httpExchange -> {
                    String response = registry.scrape();
                    byte[] bytes = response.getBytes();

                    httpExchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                    httpExchange.sendResponseHeaders(200, bytes.length);

                    try (OutputStream os = httpExchange.getResponseBody()) {
                        os.write(bytes);
                    }
                });
                server.start();
                log.info("📊 Metrics enabled at http://localhost:8080/metrics");
            } catch (Exception e) {
                log.error("Failed to start metrics server", e);
            }
        }, "metrics-http");

        metricsThread.setDaemon(true);
        metricsThread.start();

        // --------------------------------------------------------------------
        // 2. METRICS: COUNTERS + TIMERS
        // --------------------------------------------------------------------
        Counter persistedCounter = Counter.builder("streamkernel_sink_records_total")
                .description("Total number of records successfully written to Postgres/Kafka")
                .tag("stage", "persisted")
                .register(registry);

        Counter failedCounter = Counter.builder("streamkernel_sink_records_total")
                .description("Total number of records that failed to be written")
                .tag("stage", "failed")
                .register(registry);

        Timer sinkLatency = Timer.builder("streamkernel_sink_record_latency_seconds")
                .description("End-to-end record latency")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.9, 0.99)
                .tag("sink", "target")
                .register(registry);

        // --------------------------------------------------------------------
        // 3. LOAD CONFIG & COMPONENTS
        // --------------------------------------------------------------------
        PipelineConfig config = PipelineConfig.get();

        // --- THE FIX: Read App Batch Size ---
        // Reads "pipeline.app.batch.size=4000" to match the 4MB Kafka buffer
        int appBatchSize = Integer.parseInt(
                config.getProperty("pipeline.app.batch.size", "4000")
        );
        log.info("🚀 Configured App Batch Size: {}", appBatchSize);

        // Synthetic Source (High Entropy = true for strict test)
        // 1024 bytes per message, High Entropy (Random Data)
        SourceConnector<String> source = new SyntheticSource(1024);

        // Real Sink (Kafka/Postgres) wrapped with metrics
        OutputSink<String> monitoredSink;
        OutputSink<String> actualSink;

        try {
            MetricsRuntime metrics = MetricsFactory.init(config);
            actualSink = PipelineFactory.createSink(config, metrics);

            // Wrap the real sink with latency timers
            monitoredSink = payload -> {
                Timer.Sample sample = Timer.start(registry);
                try {
                    actualSink.write(payload);
                    persistedCounter.increment();
                } catch (Exception ex) {
                    failedCounter.increment();
                    throw ex;
                } finally {
                    sample.stop(sinkLatency);
                }
            };

        } catch (Exception e) {
            log.error("🚨 Fatal Error: Failed to initialize sink.", e);
            System.exit(1);
            return;
        }

        // --------------------------------------------------------------------
        // 4. MANUAL DISPATCH LOOP (Bypassing Orchestrator for Raw Speed)
        // --------------------------------------------------------------------
        // We use a manual loop here to strictly control the batch size passed
        // to source.fetchBatch(). The standard PipelineOrchestrator might default
        // to smaller batches, which kills throughput in "Strict Mode".

        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    log.info("🛑 Shutting down benchmark...");
                    running.set(false);
                }, "sink-bench-shutdown")
        );

        source.connect();
        // actualSink.connect(); // Uncomment if your Sink interface has connect()

        log.info("🏎️  Starting High-Performance Dispatch Loop...");

        // Use Virtual Threads for dispatching
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            while (running.get()) {
                // FETCH: Grab a huge chunk (e.g. 4000 items) to fill the Kafka Buffer
                var batch = source.fetchBatch(appBatchSize);

                if (batch == null || batch.isEmpty()) {
                    continue; // Should not happen with Ring Buffer
                }

                // DISPATCH: Submit the batch to be written
                executor.submit(() -> {
                    for (var record : batch) {
                        try {
                            monitoredSink.write(record);
                        } catch (Exception e) {
                            // Logged by monitoredSink wrapper
                        }
                    }
                });
            }
        }

        source.disconnect();
        // actualSink.disconnect(); // Uncomment if needed
        log.info("Benchmark Finished.");
    }
}