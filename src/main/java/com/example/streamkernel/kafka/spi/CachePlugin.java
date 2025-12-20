package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;

public interface CachePlugin {
    String id(); // e.g. "NOOP", "LOCAL", "REDIS"
    CacheStrategy<String> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}
