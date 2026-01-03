/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Objects;

/**
 * A configurable "Dead Letter Queue" sink that logs dropped events.
 * Useful for development, debugging, or low-volume pipelines where a Kafka DLQ is overkill.
 */
public final class DlqLogSinkPlugin implements SinkPlugin {

    private static final Logger log = LoggerFactory.getLogger(DlqLogSinkPlugin.class);

    // Config keys
    private static final String CFG_MAX_LOG_CHARS = "dlq.log.max.chars";
    private static final String CFG_LOG_LEVEL = "dlq.log.level";
    private static final String CFG_LOG_PAYLOAD = "dlq.log.payload.enabled";
    private static final String CFG_METRIC_NAME = "dlq.log.metric.name";

    // Defaults
    private static final int DEFAULT_MAX_LOG_CHARS = 1024;
    private static final String DEFAULT_LOG_LEVEL = "WARN";
    private static final boolean DEFAULT_LOG_PAYLOAD = true;
    private static final String DEFAULT_METRIC_NAME = "pipeline.dlq.dropped";

    @Override
    public String id() {
        return "DLQ_LOG";
    }

    @Override
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");

        // 1. Safe Config Loading
        final int maxChars = clampInt(config.getInt(CFG_MAX_LOG_CHARS, DEFAULT_MAX_LOG_CHARS), 0, 1_048_576);
        final String level = normalizeUpper(config.getString(CFG_LOG_LEVEL, DEFAULT_LOG_LEVEL));
        final boolean logPayload = parseBoolean(config.getString(CFG_LOG_PAYLOAD, Boolean.toString(DEFAULT_LOG_PAYLOAD)));
        final String metricName = normalize(config.getString(CFG_METRIC_NAME, DEFAULT_METRIC_NAME));

        // 2. Safe Metrics Fallback
        final MetricsRuntime safeMetrics = (metrics != null) ? metrics : createNoopMetrics();

        log.info("🔌 Initialized DLQ Log Plugin (Level={}, MaxChars={}, Metric={})", level, maxChars, metricName);

        // 3. Return Sink Implementation
        return new OutputSink<Object>() {
            @Override
            public void write(PipelinePayload<Object> payload) {
                if (payload == null) return;

                // Increment Metric (Alert Trigger)
                try {
                    safeMetrics.counter(metricName, 1.0);
                } catch (Exception ignored) { }

                // Check Log Level before processing string
                if (!shouldLog(level)) return;

                final String content;
                if (logPayload) {
                    // Safe toString + Truncation
                    String raw = String.valueOf(payload.data());
                    content = (raw.length() > maxChars)
                            ? raw.substring(0, maxChars) + "... [TRUNCATED]"
                            : raw;
                } else {
                    content = "[payload logging disabled]";
                }

                logAtLevel(level, "⚠️ [DLQ] Dropped Event ID: {} | Content: {}", payload.id(), content);
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }

    // --- Helpers ---

    private static boolean shouldLog(String level) {
        return switch (level) {
            case "ERROR" -> log.isErrorEnabled();
            case "WARN"  -> log.isWarnEnabled();
            case "INFO"  -> log.isInfoEnabled();
            case "DEBUG" -> log.isDebugEnabled();
            case "TRACE" -> log.isTraceEnabled();
            case "OFF"   -> false;
            default      -> log.isWarnEnabled();
        };
    }

    private static void logAtLevel(String level, String fmt, Object a, Object b) {
        switch (level) {
            case "ERROR" -> log.error(fmt, a, b);
            case "INFO"  -> log.info(fmt, a, b);
            case "DEBUG" -> log.debug(fmt, a, b);
            case "TRACE" -> log.trace(fmt, a, b);
            case "OFF"   -> { /* no-op */ }
            default      -> log.warn(fmt, a, b);
        }
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static String normalize(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String normalizeUpper(String s) {
        String n = normalize(s);
        return (n == null) ? null : n.toUpperCase(Locale.ROOT);
    }

    private static boolean parseBoolean(String s) {
        if (s == null) return false;
        return Boolean.parseBoolean(s.trim());
    }

    // Self-contained NOOP to avoid dependencies on internal classes
    private static MetricsRuntime createNoopMetrics() {
        return new MetricsRuntime() {
            @Override public Object registry() { return null; }
            @Override public boolean enabled() { return false; }
            @Override public String type() { return "NOOP"; }
            @Override public void counter(String name) { }
            @Override public void counter(String name, double increment) { }
            @Override public void timer(String name, long durationMillis) { }
            @Override public void gauge(String name, double value) { }
            @Override public void close() { }
        };
    }
}