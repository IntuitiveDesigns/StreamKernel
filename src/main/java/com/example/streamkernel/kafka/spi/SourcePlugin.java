package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;

public interface SourcePlugin {
    String id(); // e.g. "SYNTHETIC", "KAFKA", "REST"
    SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}
