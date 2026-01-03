/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.metrics;

/**
 * The vendor-agnostic contract for Observability.
 *
 * Design Philosophy:
 * This interface decouples the Core Kernel from specific implementations (Micrometer, OpenTelemetry).
 * It allows the kernel to compile and run even if no metrics library is on the classpath (via NOOP defaults).
 */
public interface MetricsRuntime extends AutoCloseable {

    /**
     * Returns the underlying registry (e.g., MeterRegistry) for advanced usage.
     * Returns Object to avoid forcing a compile-time dependency on Micrometer for the core module.
     */
    Object registry();

    /**
     * @return true if metrics are actually being recorded.
     */
    default boolean enabled() { return false; }

    /**
     * @return A string identifier for the implementation (e.g., "MICROMETER", "NOOP").
     */
    default String type() { return "NOOP"; }

    // --- Standard Instrumentation Methods (with NOOP defaults) ---

    default void counter(String name) {}

    default void counter(String name, double increment) {}

    default void timer(String name, long durationMillis) {}

    default void gauge(String name, double value) {}

    @Override
    default void close() {
        // no-op by default
    }
}