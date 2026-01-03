/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.spi;

/**
 * Runtime contract for authorization checks.
 *
 * <p><b>Performance Note:</b> This method is often called on the "Hot Path"
 * (per event). Implementations MUST use caching (e.g., Caffeine) to avoid
 * network latency on every call.</p>
 */
public interface SecurityProvider extends AutoCloseable {

    /**
     * Checks if the action is authorized.
     *
     * @param identity The actor (e.g. "service-account-a" or "user:123")
     * @param action   The operation (e.g. "write", "read", "delete")
     * @param resource The target (e.g. "topic:sales-events", "index:users")
     * @return true if allowed, false if denied.
     */
    boolean isAllowed(String identity, String action, String resource);

    /**
     * Lifecycle hook to clean up resources (HTTP clients, thread pools).
     * Default is no-op for stateless providers (like AllowAll).
     */
    @Override
    default void close() {
        // No-op by default
    }
}