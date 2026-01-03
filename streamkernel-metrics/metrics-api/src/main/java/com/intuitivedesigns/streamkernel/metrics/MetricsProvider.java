/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.metrics;

/**
 * Service Provider Interface (SPI) for Metrics implementations.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}.
 * To add a new metrics backend (e.g., Prometheus, Datadog), implement this interface
 * and register it in {@code META-INF/services/com.intuitivedesigns.streamkernel.metrics.MetricsProvider}.
 */
public interface MetricsProvider {

    /**
     * The unique identifier for this provider (e.g., "PROMETHEUS", "DATADOG").
     * <p>This ID is used to match against the {@code metrics.type} configuration.
     */
    String id();

    /**
     * Creates a runtime instance for this provider if the settings match.
     *
     * @param settings The configuration settings.
     * @return A valid {@link MetricsRuntime} if this provider is selected,
     * or {@code null} if the provider should be skipped.
     */
    MetricsRuntime create(MetricsSettings settings);

    /**
     * Helper to check if this provider is the one requested in config.
     *
     * @param configuredId The value from {@code metrics.type}.
     * @return true if the IDs match (case-insensitive).
     */
    default boolean matches(String configuredId) {
        return configuredId != null && id().equalsIgnoreCase(configuredId.trim());
    }
}