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