/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.metrics;

public final class NoopMetricsProvider implements MetricsProvider {

    @Override
    public String id() {
        return "NOOP";
    }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        // Do not hijack the factory if another provider (e.g. DATADOG) is requested.
        // This only activates if 'metrics.provider=NOOP' (or case-insensitive match).
        if (s == null || !matches(s.providerId)) {
            return null;
        }
        return NoopMetricsRuntime.INSTANCE;
    }

    private static final class NoopMetricsRuntime implements MetricsRuntime {
        static final NoopMetricsRuntime INSTANCE = new NoopMetricsRuntime();

        // Sentinel object to prevent NPEs in "instanceof" checks downstream
        private final Object sentinelRegistry = new Object();

        @Override
        public Object registry() {
            return sentinelRegistry;
        }

        @Override public boolean enabled() { return false; }
        @Override public String type() { return "NOOP"; }

        // Default interface methods for counter/timer/gauge/close usually do nothing,
        // but if your MetricsRuntime interface requires implementation, ensure they are here.
        @Override public void close() { /* no-op */ }
    }
}