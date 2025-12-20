package com.example.streamkernel.kafka.config;

import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.cache.LocalCacheStrategy;
import com.example.streamkernel.kafka.cache.RedisCacheStrategy;

public class CacheFactory {

    public enum CacheType { LOCAL, REDIS }

    public static <T> CacheStrategy<T> getCache(CacheType type) {
        return switch (type) {
            case LOCAL -> new LocalCacheStrategy<>();
            case REDIS -> new RedisCacheStrategy<>("localhost", 6379);
        };
    }
}