/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.CachePlugin;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * A No-Op (Pass-through) Cache implementation.
 * Always returns 'empty' on lookups and discards writes.
 * Used for benchmarking or pipelines that require no state.
 * <p>
 * ID: NOOP
 */
public final class NoopCachePlugin implements CachePlugin {

    public static final String ID = "NOOP";
    private static final Logger log = LoggerFactory.getLogger(NoopCachePlugin.class);

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.CACHE;
    }

    @Override
    public Cache<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        log.info("Creating No-Op Cache (Stateless)");
        return new NoopCache<>();
    }

    private static final class NoopCache<K, V> implements Cache<K, V> {
        
        @Override
        public Optional<V> get(K key) {
            // Always a cache miss
            return Optional.empty();
        }

        @Override
        public void put(K key, V value) {
            // No-op: do not store
        }

        @Override
        public void invalidate(K key) {
            // No-op
        }
        
        @Override
        public void close() {
            // No-op
        }
    }
}
