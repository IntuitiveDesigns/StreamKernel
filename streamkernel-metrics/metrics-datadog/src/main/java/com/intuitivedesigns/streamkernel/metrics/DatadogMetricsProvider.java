/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.metrics;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.datadog.DatadogConfig;
import io.micrometer.datadog.DatadogMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public final class DatadogMetricsProvider implements MetricsProvider {

    private static final Logger log = LoggerFactory.getLogger(DatadogMetricsProvider.class);

    private static final String DEFAULT_URI = "https://api.datadoghq.com";
    private static final Duration DEFAULT_STEP = Duration.ofSeconds(10);

    @Override
    public String id() {
        return "DATADOG";
    }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        // SPI contract: return null if not applicable
        if (s == null || !matches(s.providerId)) {
            return null;
        }

        final String apiKey = normalize(s.datadogApiKey);
        if (apiKey == null) {
            // Return null so MetricsFactory falls back to NOOP cleanly
            log.error("Datadog metrics selected but API key is missing (DD_API_KEY or metrics.datadog.apiKey).");
            return null;
        }

        final String uri = normalizeOrDefault(s.datadogUri, DEFAULT_URI);
        final Duration step = sanitizeStep(s.step);
        final String hostTag = normalize(s.datadogHostTag);

        final DatadogConfig cfg = new DatadogConfig() {
            @Override public String get(String k) { return null; }
            @Override public String apiKey() { return apiKey; }
            @Override public String uri() { return uri; }
            @Override public Duration step() { return step; }
            @Override public String hostTag() { return hostTag; }
        };

        try {
            final DatadogMeterRegistry reg = new DatadogMeterRegistry(cfg, Clock.SYSTEM);

            // Apply common tags (env, service, etc.)
            MetricsUtil.applyCommonTags(reg, s);

            log.info("✅ Datadog Metrics Active (uri={}, step={}s, hostTag={})",
                    uri, step.getSeconds(), hostTag == null ? "auto" : hostTag);

            return new DatadogRuntime(reg);
        } catch (Exception e) {
            log.error("Failed to initialize Datadog registry", e);
            return null;
        }
    }

    private static Duration sanitizeStep(Duration step) {
        if (step == null || step.isNegative() || step.isZero()) return DEFAULT_STEP;
        return step;
    }

    // --- Runtime Wrapper ---

    private static final class DatadogRuntime implements MetricsRuntime {
        private final DatadogMeterRegistry registry;

        DatadogRuntime(DatadogMeterRegistry registry) {
            this.registry = registry;
        }

        @Override
        public MeterRegistry registry() {
            return registry;
        }

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public String type() {
            return "DATADOG";
        }

        @Override
        public void close() {
            try {
                registry.close();
            } catch (Exception ignored) {
            }
        }
    }

    // --- Helpers ---

    private static String normalize(String s) {
        if (s == null) return null;
        s = s.trim();
        return s.isEmpty() ? null : s;
    }

    private static String normalizeOrDefault(String s, String def) {
        final String n = normalize(s);
        return (n != null) ? n : def;
    }
}
