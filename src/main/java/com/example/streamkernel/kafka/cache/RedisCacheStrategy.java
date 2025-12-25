/*
 * Copyright 2025 Steven Lopez
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.example.streamkernel.kafka.cache;

import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.core.PipelinePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.Optional;

/**
 * Thread-safe Redis Cache Strategy using JedisPool.
 * compatible with Virtual Threads (Blocking I/O).
 */
public class RedisCacheStrategy<T> implements CacheStrategy<T> {

    private static final Logger log = LoggerFactory.getLogger(RedisCacheStrategy.class);

    private final JedisPool jedisPool;
    private final String host;
    private final int port;

    // 1. Default Constructor
    public RedisCacheStrategy() {
        this("localhost", 6379);
    }

    // 2. Argument Constructor
    public RedisCacheStrategy(String host, int port) {
        this.host = host;
        this.port = port;
        this.jedisPool = initPool(host, port);
    }

    private JedisPool initPool(String host, int port) {
        JedisPoolConfig config = new JedisPoolConfig();
        // Vital for Virtual Threads: Ensure the pool is large enough
        // to not become a bottleneck, or use "blockWhenExhausted"
        config.setMaxTotal(128);
        config.setMaxIdle(16);
        config.setMinIdle(4);
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);
        config.setTestWhileIdle(true);

        log.info("🔌 Connecting to Redis at {}:{} [Pool Size: {}]", host, port, config.getMaxTotal());
        return new JedisPool(config, host, port, 2000); // 2s timeout
    }

    @Override
    public void put(String key, PipelinePayload<T> value) {
        // Rent a connection from the pool
        try (Jedis jedis = jedisPool.getResource()) {
            // SIMPLE SERIALIZATION:
            // For a real app, use Jackson/Gson to serialize 'value' to JSON.
            // Here we just store the ID or Data as a string for the demo.
            String valueToStore = value.data() != null ? value.data().toString() : "";

            jedis.set(key, valueToStore);

            // Optional: Set TTL (e.g., 1 hour)
            // jedis.expire(key, 3600);
        } catch (Exception e) {
            log.error("Failed to PUT to Redis: {}", key, e);
        }
    }

    @Override
    public Optional<PipelinePayload<T>> get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            String val = jedis.get(key);

            if (val == null) {
                return Optional.empty();
            }

            // DESERIALIZATION:
            // You would reconstruct the PipelinePayload<T> from JSON here.
            // Returning null/stub because we lack the constructor context for T.
            return Optional.empty();

        } catch (Exception e) {
            log.error("Failed to GET from Redis: {}", key, e);
            return Optional.empty();
        }
    }

    @Override
    public void remove(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        } catch (Exception e) {
            log.error("Failed to DELETE from Redis: {}", key, e);
        }
    }

    // Call this if your app supports cache lifecycle shutdown
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            log.info("🔌 Closing Redis Pool...");
            jedisPool.close();
        }
    }
}