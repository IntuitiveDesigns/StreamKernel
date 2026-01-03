/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.sources;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Production-Grade REST Polling Source.
 * Fetches data from an HTTP Endpoint at a fixed interval.
 */
public final class RestSourceConnector implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(RestSourceConnector.class);

    private final HttpClient client;
    private final URI targetUri;
    private final Duration pollInterval;
    private final MetricsRuntime metrics;

    // Internal buffer to decouple HTTP latency from Pipeline throughput
    private final Queue<PipelinePayload<String>> buffer = new ConcurrentLinkedQueue<>();

    private Instant nextPollTime = Instant.now();

    private RestSourceConnector(String url, Duration pollInterval, MetricsRuntime metrics) {
        this.targetUri = URI.create(url);
        this.pollInterval = pollInterval;
        this.metrics = metrics;

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_2) // Auto-downgrade to 1.1 if needed
                .build();
    }

    public static RestSourceConnector fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");

        String url = config.getString("source.rest.url", null);
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("Missing config: source.rest.url");
        }

        long pollMs = config.getLong("source.rest.poll.interval.ms", 5000L); // Default 5s

        return new RestSourceConnector(url, Duration.ofMillis(pollMs), metrics);
    }

    @Override
    public void connect() {
        log.info("🌎 REST Source Connected. Target: {} (Poll every {}ms)", targetUri, pollInterval.toMillis());
    }

    @Override
    public void disconnect() {
        log.info("🌎 REST Source Disconnected.");
        // HttpClient resources are managed by JVM (idle connection eviction)
    }

    @Override
    public PipelinePayload<String> fetch() {
        // 1. Drain Buffer first
        if (!buffer.isEmpty()) {
            return buffer.poll();
        }

        // 2. Check if it's time to poll
        if (Instant.now().isBefore(nextPollTime)) {
            return null; // Yield to kernel (idle)
        }

        // 3. Poll API
        performHttpCall();

        // Schedule next poll
        nextPollTime = Instant.now().plus(pollInterval);

        return buffer.poll();
    }

    private void performHttpCall() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(targetUri)
                    .GET()
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .header("User-Agent", "StreamKernel/1.0")
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                // For this generic connector, we treat the whole body as one payload.
                // In a real app, you might parse a JSON Array here and add multiple items to the buffer.
                String body = response.body();
                if (body != null && !body.isBlank()) {
                    buffer.add(new PipelinePayload<>(
                            UUID.randomUUID().toString(), // Or hash of body
                            body,
                            Instant.now(),
                            null
                    ));
                    metrics.counter("source.rest.fetch.ok", 1.0);
                }
            } else {
                log.warn("REST Poll Failed. Status: {}", response.statusCode());
                metrics.counter("source.rest.fetch.fail", 1.0);
            }
        } catch (Exception e) {
            log.error("REST Connection Error", e);
            metrics.counter("source.rest.fetch.error", 1.0);
        }
    }
}