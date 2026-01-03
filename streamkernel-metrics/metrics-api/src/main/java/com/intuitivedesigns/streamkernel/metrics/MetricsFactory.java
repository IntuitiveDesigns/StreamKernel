/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.ServiceLoader;

public final class MetricsFactory {

    private static final Logger log = LoggerFactory.getLogger(MetricsFactory.class);

    private static final MetricsRuntime NOOP = new NoopMetricsRuntime();

    private MetricsFactory() {}

    public static MetricsRuntime init(MetricsSettings settings) {
        Objects.requireNonNull(settings, "settings");

        final ClassLoader cl = resolveClassLoader();

        // Load SPI services using the context classloader to support
        // complex environments (OSGi, Spring Boot, Containers).
        final ServiceLoader<MetricsProvider> loader = ServiceLoader.load(MetricsProvider.class, cl);

        for (MetricsProvider p : loader) {
            try {
                // Providers should check config (e.g. "metrics.type") and return null if they don't match.
                final MetricsRuntime rt = p.create(settings);
                if (rt != null) {
                    log.info("✅ Metrics Runtime initialized: {}", p.getClass().getName());
                    return rt;
                }
            } catch (Throwable t) {
                // CRITICAL: Catch Throwable (not just Exception) to handle NoClassDefFoundError
                // or LinkageError if a provider is missing dependencies.
                log.warn("Failed to initialize metrics provider [{}]: {}", p.getClass().getName(), t.getMessage());
                log.debug("Provider init stack trace:", t);
            }
        }

        log.info("Metrics disabled or no suitable provider found (NOOP active).");
        return NOOP;
    }

    private static ClassLoader resolveClassLoader() {
        final ClassLoader threadCl = Thread.currentThread().getContextClassLoader();
        return (threadCl != null) ? threadCl : MetricsFactory.class.getClassLoader();
    }

    // --- Safe Fallback ---

    private static final class NoopMetricsRuntime implements MetricsRuntime {
        // Return a sentinel object instead of null to prevent NPEs in "instanceof" checks downstream
        private final Object sentinelRegistry = new Object();

        @Override
        public Object registry() {
            return sentinelRegistry;
        }

        @Override public void counter(String name) { /* no-op */ }
        @Override public void counter(String name, double increment) { /* no-op */ }
        @Override public void timer(String name, long durationMillis) { /* no-op */ }
        @Override public void gauge(String name, double value) { /* no-op */ }

        @Override
        public void close() { /* no-op */ }
    }
}