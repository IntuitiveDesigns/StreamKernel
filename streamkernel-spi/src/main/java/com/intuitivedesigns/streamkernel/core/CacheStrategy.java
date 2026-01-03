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

package com.intuitivedesigns.streamkernel.core;

import java.util.Optional;

/**
 * Optional caching layer for enrichment, deduplication, throttling,
 * routing decisions, or stateful processing.
 *
 * Examples:
 * - Local in-memory cache for extremely fast lookups
 * - Redis-based distributed cache
 * - Metadata lookup before transformation
 * - Idempotency or exactly-once state tracking
 *
 * CacheStrategy is intentionally minimal so connectors can plug in any
 * implementation without impacting the pipeline architecture.
 *
 * @param <T> The type of data being cached.
 */
public interface CacheStrategy<T> {

    /**
     * Initialize cache resources.
     * <p>
     * Most simple implementations (like LocalCacheStrategy)
     * can leave this method empty.
     * <p>
     * Distributed caches (Redis, Hazelcast, Memcached, etc.)
     * should open their client connections here.
     *
     * @throws Exception if initialization fails
     */
    default void init() throws Exception {
        // Default: no-op
    }

    /**
     * Store a payload in the cache.
     *
     * <p><b>Thread-safety Contract:</b></p>
     * Implementations must be safe for concurrent writes, since many
     * virtual threads may call this simultaneously.
     *
     * @param key A stable cache key
     * @param value The payload envelope
     * @throws Exception if the write fails
     */
    void put(String key, PipelinePayload<T> value) throws Exception;

    /**
     * Retrieve a payload from the cache.
     *
     * <p><b>Thread-safety Contract:</b></p>
     * Must safely handle concurrent reads.
     *
     * @param key cache key
     * @return Optional.empty() if not found
     * @throws Exception if the read fails
     */
    Optional<PipelinePayload<T>> get(String key) throws Exception;

    /**
     * Remove an entry from the cache.
     *
     * <p><b>Failure Contract:</b></p>
     * Throw an exception if the delete fails
     * (distributed cache unavailability, etc.)
     *
     * @param key item to remove
     * @throws Exception if deletion fails
     */
    void remove(String key) throws Exception;

    /**
     * Clean up resources (optional).
     * <p>
     * For in-memory caches: leave default implementation.
     * For distributed caches: close client connections or thread pools.
     *
     * @throws Exception if cleanup fails
     */
    default void close() throws Exception {
        // Default: no-op
    }
}


