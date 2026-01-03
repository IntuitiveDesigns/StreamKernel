/*
 * Copyright 2026 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SecurityPlugin;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Security Provider that delegates authorization decisions to Open Policy Agent (OPA) via the OPA Data API.
 *
 * Enterprise characteristics:
 * - Fail-closed by default (configurable via security.opa.fail.open)
 * - Short-lived cache with TTL to reduce network overhead
 * - Basic bounded-cache guard to prevent unbounded growth (gated flush)
 * - Metrics and (light) log throttling to preserve operability during outages
 */
public final class OpaSidecarPlugin implements SecurityPlugin {

    public static final String ID = "OPA_SIDECAR";
    private static final Logger log = LoggerFactory.getLogger(OpaSidecarPlugin.class);

    @Override
    public String id() { return ID; }

    @Override
    public PluginKind kind() { return PluginKind.SECURITY; }

    @Override
    public SecurityProvider create(PipelineConfig config, MetricsRuntime metrics) {
        String opaUrl = config.getString("security.opa.url", "http://localhost:8181/v1/data/streamkernel/authz/allow");

        long ttlMs = config.getLong("security.opa.cache.ttl.ms", 30_000L);
        boolean failOpen = config.getBoolean("security.opa.fail.open", false);

        long connectTimeoutMs = config.getLong("security.opa.http.connect.timeout.ms", 500L);
        long requestTimeoutMs = config.getLong("security.opa.http.request.timeout.ms", 2_000L);

        int maxCacheSize = (int) config.getLong("security.opa.cache.max.size", 10_000L);

        log.info("Initializing OPA Provider: url={} ttlMs={} failOpen={} maxCacheSize={}",
                opaUrl, ttlMs, failOpen, maxCacheSize);

        return new OpaProvider(
                opaUrl,
                ttlMs,
                maxCacheSize,
                failOpen,
                connectTimeoutMs,
                requestTimeoutMs,
                metrics
        );
    }

    private static final class OpaProvider implements SecurityProvider {

        private static final String CONTENT_TYPE = "Content-Type";
        private static final String JSON_CT = "application/json";

        // Log throttling: at most one ERROR per window when OPA is down
        private static final long ERROR_LOG_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(10);

        private final HttpClient client;
        private final URI uri;
        private final ObjectMapper json;

        private final long ttlNanos;
        private final int maxCacheSize;
        private final boolean failOpen;
        private final long requestTimeoutMs;

        private final MetricsRuntime metrics;

        private final Map<String, CacheEntry> decisionCache = new ConcurrentHashMap<>();
        private final LongAdder approxCacheEntries = new LongAdder();
        private final AtomicBoolean isClearing = new AtomicBoolean(false);

        private final AtomicLong nextErrorLogAtNanos = new AtomicLong(0);

        OpaProvider(
                String url,
                long ttlMs,
                int maxCacheSize,
                boolean failOpen,
                long connectTimeoutMs,
                long requestTimeoutMs,
                MetricsRuntime metrics
        ) {
            if (ttlMs < 0) throw new IllegalArgumentException("security.opa.cache.ttl.ms must be >= 0");
            if (maxCacheSize <= 0) throw new IllegalArgumentException("security.opa.cache.max.size must be > 0");

            this.client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                    .build();

            this.uri = URI.create(url);
            this.json = new ObjectMapper();

            this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMs);
            this.maxCacheSize = maxCacheSize;
            this.failOpen = failOpen;
            this.requestTimeoutMs = requestTimeoutMs;

            this.metrics = metrics;
        }

        @Override
        public boolean isAllowed(String principal, String action, String resource) {
            final String key = principal + "|" + action + "|" + resource;
            final long nowNanos = System.nanoTime();

            // 1) Cache check
            CacheEntry cached = decisionCache.get(key);
            if (cached != null && nowNanos < cached.expiresAtNanos) {
                counter("opa_cache_hit_total", 1);
                return cached.allowed;
            }
            counter("opa_cache_miss_total", 1);

            // 2) Remote call
            Boolean decision = callOpa(principal, action, resource);

            // If callOpa returns null => error path
            final boolean allowed;
            if (decision == null) {
                counter("opa_call_error_total", 1);
                allowed = failOpen;
            } else {
                allowed = decision;
                counter("opa_call_success_total", 1);
                counter(allowed ? "opa_allow_total" : "opa_deny_total", 1);
            }

            // 3) Cache update
            if (ttlNanos > 0) {
                maybeBoundCacheAndEvict();
                long expiresAt = nowNanos + ttlNanos;
                CacheEntry prev = decisionCache.put(key, new CacheEntry(allowed, expiresAt));
                if (prev == null) {
                    approxCacheEntries.increment();
                }
            }

            return allowed;
        }

        private void maybeBoundCacheAndEvict() {
            if (approxCacheEntries.sum() < maxCacheSize) return;

            if (!isClearing.compareAndSet(false, true)) return;

            try {
                // Gated flush logic
                long now = System.nanoTime();
                int removedExpired = 0;
                for (var it = decisionCache.entrySet().iterator(); it.hasNext(); ) {
                    var e = it.next();
                    CacheEntry ce = e.getValue();
                    if (ce != null && now >= ce.expiresAtNanos) {
                        it.remove();
                        removedExpired++;
                        if (removedExpired >= 1024) break;
                    }
                }

                if (removedExpired > 0) {
                    counter("opa_cache_expired_evictions_total", removedExpired);
                }

                if (approxCacheEntries.sum() >= maxCacheSize) {
                    decisionCache.clear();
                    approxCacheEntries.reset();
                    counter("opa_cache_flush_total", 1);
                    log.warn("OPA cache reached max size ({}). Flushed.", maxCacheSize);
                }
            } finally {
                isClearing.set(false);
            }
        }

        private Boolean callOpa(String principal, String action, String resource) {
            final long startNanos = System.nanoTime();
            try {
                ObjectNode root = json.createObjectNode();
                ObjectNode input = root.putObject("input");
                input.put("user", principal);
                input.put("action", action);
                input.put("resource", resource);

                byte[] requestBody = json.writeValueAsBytes(root);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .header(CONTENT_TYPE, JSON_CT)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
                        .timeout(Duration.ofMillis(requestTimeoutMs))
                        .build();

                HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

                long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                timer("opa_request_latency_ms", durationMs);

                if (response.statusCode() != 200) {
                    counter("opa_http_status_total", 1);
                    throttledWarn("OPA sidecar returned HTTP {}", response.statusCode());
                    return null;
                }

                JsonNode resultNode = json.readTree(response.body());
                if (!resultNode.has("result")) {
                    counter("opa_parse_error_total", 1);
                    throttledWarn("OPA response missing 'result' field", null);
                    return null;
                }

                return resultNode.get("result").asBoolean();

            } catch (Exception e) {
                long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                timer("opa_request_latency_ms", durationMs);
                throttledError("Failed to contact OPA sidecar", e);
                return null;
            }
        }

        private void throttledWarn(String msg, Object arg) {
            log.warn(msg, arg);
        }

        private void throttledError(String msg, Exception e) {
            long now = System.nanoTime();
            long nextAt = nextErrorLogAtNanos.get();
            if (now >= nextAt && nextErrorLogAtNanos.compareAndSet(nextAt, now + ERROR_LOG_WINDOW_NANOS)) {
                log.error(msg, e);
            } else {
                counter("opa_error_log_suppressed_total", 1);
            }
        }

        private void counter(String name, long inc) {
            if (metrics == null) return;
            metrics.counter(name, (double) inc);
        }

        private void timer(String name, long durationMs) {
            if (metrics == null) return;
            metrics.timer(name, durationMs);
        }

        @Override
        public void close() {
            decisionCache.clear();
            approxCacheEntries.reset();
        }

        private record CacheEntry(boolean allowed, long expiresAtNanos) {}
    }
}
