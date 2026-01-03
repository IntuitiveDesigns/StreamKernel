/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.CachePlugin;
import com.intuitivedesigns.streamkernel.spi.PluginKind;

import java.util.Optional;

public final class NoopCachePlugin implements CachePlugin {

    public static final String ID = "NOOP";

    @Override
    public String id() { return ID; }

    @Override
    public PluginKind kind() { return PluginKind.CACHE; }

    @Override
    public Cache<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        return new NoopCache<>();
    }

    private static final class NoopCache<K, V> implements Cache<K, V> {
        @Override
        public Optional<V> get(K key) { return Optional.empty(); }

        @Override
        public void put(K key, V value) { /* No-op */ }

        @Override
        public void invalidate(K key) { /* No-op */ }
        
        @Override
        public void close() { /* No-op */ }
    }
}
