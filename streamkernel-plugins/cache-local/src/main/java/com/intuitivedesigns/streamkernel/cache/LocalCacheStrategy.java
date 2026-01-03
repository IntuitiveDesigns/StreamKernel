/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.cache;

import com.intuitivedesigns.streamkernel.core.CacheStrategy;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local in-memory cache strategy (Optimized).
 *
 * Characteristics:
 * - Thread-safe (ConcurrentHashMap)
 * - Non-evicting (No LRU overhead)
 * - Bounded (Prevents OOM via maxCapacity safety valve)
 *
 * Trade-off:
 * This strategy prioritizes raw speed over hit-rate. It does not perform eviction.
 * When the cache is full, new entries are simply ignored ("Best Effort").
 */
public final class LocalCacheStrategy<T> implements CacheStrategy<T> {

    private static final Logger log = LoggerFactory.getLogger(LocalCacheStrategy.class);

    private static final int DEFAULT_INITIAL = 256;
    private static final int DEFAULT_MAX = 100_000;

    private final Map<String, PipelinePayload<T>> cache;
    private final int maxCapacity;

    /**
     * Default constructor (Safe bounds).
     */
    public LocalCacheStrategy() {
        this(DEFAULT_INITIAL, DEFAULT_MAX);
    }

    /**
     * Tuning constructor.
     *
     * @param initialCapacity Starting size (avoids resizing overhead).
     * @param maxCapacity     Hard limit to prevent OutOfMemoryError.
     */
    public LocalCacheStrategy(int initialCapacity, int maxCapacity) {
        this.maxCapacity = maxCapacity;
        // Load Factor 0.75 is standard. Concurrency level inferred by Java 8+.
        this.cache = new ConcurrentHashMap<>(initialCapacity);
    }

    @Override
    public void put(String key, PipelinePayload<T> value) {
        if (key == null || value == null) return;

        // OPTIMIZATION: Safety Valve
        // If we hit the limit, stop accepting new items.
        // This is faster than calculating LRU eviction for a "simple" cache.
        if (cache.size() >= maxCapacity && !cache.containsKey(key)) {
            // Optional: Log once every N times or just silently drop for performance
            return;
        }

        cache.put(key, value);
    }

    @Override
    public Optional<PipelinePayload<T>> get(String key) {
        if (key == null) return Optional.empty();
        return Optional.ofNullable(cache.get(key));
    }

    @Override
    public void remove(String key) {
        if (key == null) return;
        cache.remove(key);
    }

    /**
     * Explicit clear for lifecycle control or manual invalidation.
     */
    public void clear() {
        cache.clear();
        log.debug("Local cache cleared.");
    }

    /**
     * Observability helper.
     */
    public int size() {
        return cache.size();
    }
}