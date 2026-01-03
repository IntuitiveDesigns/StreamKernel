/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */
package com.intuitivedesigns.streamkernel.spi;

import java.util.Optional;

/**
 * Standard interface for caching mechanisms.
 * Located in SPI to be accessible by all plugins.
 */
public interface Cache<K, V> extends AutoCloseable {
    Optional<V> get(K key);
    void put(K key, V value);
    void invalidate(K key);
    
    @Override 
    default void close() {
        // Default no-op
    }
}
