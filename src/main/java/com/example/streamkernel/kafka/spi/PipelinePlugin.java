package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;

public interface PipelinePlugin {
    String id();          // e.g. "KAFKA", "SYNTHETIC", "POSTGRES"
    PluginKind kind();    // SOURCE / SINK / TRANSFORMER / CACHE

    Object create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}
