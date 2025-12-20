package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.cache.LocalCacheStrategy;
import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.CachePlugin;

public final class LocalCachePlugin implements CachePlugin {

    @Override
    public String id() {
        return "LOCAL";
    }

    @Override
    public CacheStrategy<String> create(PipelineConfig config, MetricsRuntime metrics) {
        return new LocalCacheStrategy<>();
    }
}
