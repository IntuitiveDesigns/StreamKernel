/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.CachePlugin;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public final class LocalCachePlugin implements CachePlugin {

    public static final String ID = "LOCAL_CAFFEINE";
    private static final Logger log = LoggerFactory.getLogger(LocalCachePlugin.class);

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
        long maxSize = config.getLong("cache.local.max.size", 10_000);
        long ttlSec = config.getLong("cache.local.ttl.seconds", 300);
        
        log.info("Creating Local Cache (Size={}, TTL={}s)", maxSize, ttlSec);
        return new LocalCache<>(maxSize, ttlSec);
    }

    // Inner implementation adapting Caffeine to SPI Cache
    private static final class LocalCache<K, V> implements Cache<K, V> {
        private final com.github.benmanes.caffeine.cache.Cache<K, V> underlying;

        LocalCache(long maxSize, long ttlSeconds) {
            this.underlying = Caffeine.newBuilder()
                    .maximumSize(maxSize)
                    .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                    .build();
        }

        @Override
        public Optional<V> get(K key) {
            return Optional.ofNullable(underlying.getIfPresent(key));
        }

        @Override
        public void put(K key, V value) {
            if (key != null && value != null) {
                underlying.put(key, value);
            }
        }

        @Override
        public void invalidate(K key) {
            underlying.invalidate(key);
        }

        @Override
        public void close() {
            underlying.invalidateAll();
            underlying.cleanUp();
        }
    }
}
