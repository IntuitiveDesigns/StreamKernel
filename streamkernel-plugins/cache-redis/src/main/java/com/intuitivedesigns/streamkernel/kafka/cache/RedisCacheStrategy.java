/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.cache;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.CacheStrategy;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Production-Grade Redis Cache Strategy.
 * Features:
 * - JedisPool for high-throughput concurrency
 * - Config-driven factory
 * - Built-in "Zero-Dep" Base64 Protocol for String payloads
 * - Metrics Integration
 */
public final class RedisCacheStrategy<T> implements CacheStrategy<T>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RedisCacheStrategy.class);
    private static final String PROTOCOL_V1 = "V1";

    private final JedisPool jedisPool;
    private final int ttlSeconds;
    private final Function<PipelinePayload<T>, String> serializer;
    private final Function<String, PipelinePayload<T>> deserializer;
    private final MetricsRuntime metrics;

    /**
     * Primary Constructor.
     */
    public RedisCacheStrategy(JedisPool pool,
                              int ttlSeconds,
                              Function<PipelinePayload<T>, String> serializer,
                              Function<String, PipelinePayload<T>> deserializer,
                              MetricsRuntime metrics) {
        this.jedisPool = pool;
        this.ttlSeconds = ttlSeconds;
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.metrics = metrics;
    }

    /**
     * Factory: Creates a Redis Strategy specifically for String payloads.
     * Includes a binary-safe Base64 serialization protocol automatically.
     */
    public static RedisCacheStrategy<String> fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        // 1. Config
        String host = config.getString("redis.host", "localhost");
        int port = config.getInt("redis.port", 6379);
        String password = config.getString("redis.password", null);
        int timeout = config.getInt("redis.timeout.ms", 2000);
        int ttl = config.getInt("redis.ttl.seconds", 3600);
        int maxTotal = config.getInt("redis.pool.max", 128);

        // 2. Pool Setup
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(config.getInt("redis.pool.idle", 16));
        poolConfig.setMinIdle(config.getInt("redis.pool.min", 4));
        poolConfig.setTestOnBorrow(false); // fast borrow
        poolConfig.setTestWhileIdle(true); // health check in background

        JedisPool pool;
        if (password != null && !password.isBlank()) {
            pool = new JedisPool(poolConfig, host, port, timeout, password);
        } else {
            pool = new JedisPool(poolConfig, host, port, timeout);
        }

        log.info("✅ Redis Cache Active: {}:{} (TTL: {}s)", host, port, ttl);

        // 3. Return Strategy with Built-in String Serializers
        return new RedisCacheStrategy<>(pool, ttl, RedisCacheStrategy::serializeString, RedisCacheStrategy::deserializeString, metrics);
    }

    @Override
    public void put(String key, PipelinePayload<T> value) {
        if (key == null || value == null) return;
        long start = System.nanoTime();

        try (Jedis jedis = jedisPool.getResource()) {
            String payload = serializer.apply(value);

            // Atomic Set-with-Expiry
            if (ttlSeconds > 0) {
                jedis.setex(key, ttlSeconds, payload);
            } else {
                jedis.set(key, payload);
            }

            if (metrics != null) metrics.counter("cache.writes", 1.0);
        } catch (Exception e) {
            log.error("Redis PUT failed key={}", key, e);
            if (metrics != null) metrics.counter("cache.errors", 1.0);
        } finally {
            recordLatency(start);
        }
    }

    @Override
    public Optional<PipelinePayload<T>> get(String key) {
        if (key == null) return Optional.empty();
        long start = System.nanoTime();

        try (Jedis jedis = jedisPool.getResource()) {
            String raw = jedis.get(key);

            if (raw == null) {
                if (metrics != null) metrics.counter("cache.misses", 1.0);
                return Optional.empty();
            }

            if (metrics != null) metrics.counter("cache.hits", 1.0);
            return Optional.ofNullable(deserializer.apply(raw));

        } catch (Exception e) {
            log.error("Redis GET failed key={}", key, e);
            if (metrics != null) metrics.counter("cache.errors", 1.0);
            return Optional.empty();
        } finally {
            recordLatency(start);
        }
    }

    @Override
    public void remove(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        } catch (Exception e) {
            log.warn("Redis DEL failed key={}", key, e);
        }
    }

    @Override
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }

    private void recordLatency(long startNs) {
        if (metrics != null) {
            metrics.timer("cache.latency", (System.nanoTime() - startNs) / 1_000_000);
        }
    }

    // --- Built-in V1 Protocol for Strings (Zero Dependencies) ---
    // Format: "V1:<Base64-ID>:<Epoch>:<Base64-Headers>:<Base64-Data>"

    private static String serializeString(PipelinePayload<String> p) {
        return PROTOCOL_V1 + ":" +
                toBase64(p.id()) + ":" +
                p.timestamp().toEpochMilli() + ":" +
                encodeHeaders(p.headers()) + ":" +
                toBase64(p.data());
    }

    private static PipelinePayload<String> deserializeString(String raw) {
        try {
            String[] parts = raw.split(":");
            if (parts.length < 5 || !PROTOCOL_V1.equals(parts[0])) return null;

            String id = fromBase64(parts[1]);
            long epoch = Long.parseLong(parts[2]);
            Map<String, String> headers = decodeHeaders(parts[3]);
            String data = fromBase64(parts[4]);

            return new PipelinePayload<>(id, data, Instant.ofEpochMilli(epoch), headers);
        } catch (Exception e) {
            return null; // Treat corruption as a cache miss
        }
    }

    private static String encodeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) return "";
        // Simple k=v,k2=v2 encoding wrapped in Base64
        String joined = headers.entrySet().stream()
                .map(e -> escape(e.getKey()) + "=" + escape(e.getValue()))
                .collect(Collectors.joining(","));
        return toBase64(joined);
    }

    private static Map<String, String> decodeHeaders(String b64) {
        String decoded = fromBase64(b64);
        if (decoded.isEmpty()) return new HashMap<>();
        Map<String, String> m = new HashMap<>();
        for (String pair : decoded.split(",")) {
            int idx = pair.indexOf('=');
            if (idx > 0) m.put(unescape(pair.substring(0, idx)), unescape(pair.substring(idx + 1)));
        }
        return m;
    }

    private static String toBase64(String s) {
        return (s == null) ? "" : Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String fromBase64(String s) {
        return (s == null || s.isEmpty()) ? "" : new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }

    private static String escape(String s) { return s.replace(",", "%2C").replace("=", "%3D"); }
    private static String unescape(String s) { return s.replace("%2C", ",").replace("%3D", "="); }
}