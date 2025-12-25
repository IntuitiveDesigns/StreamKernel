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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package com.example.streamkernel.kafka.cache;

import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.core.PipelinePayload;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class LocalCacheStrategy<T> implements CacheStrategy<T> {

    private final Map<String, PipelinePayload<T>> cache = new ConcurrentHashMap<>();

    @Override
    public void put(String key, PipelinePayload<T> value) {
        cache.put(key, value);
    }

    @Override
    public Optional<PipelinePayload<T>> get(String key) {
        return Optional.ofNullable(cache.get(key));
    }

    @Override
    public void remove(String key) {
        cache.remove(key);
    }
}