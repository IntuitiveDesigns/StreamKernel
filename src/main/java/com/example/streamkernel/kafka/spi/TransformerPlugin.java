package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.Transformer;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;

public interface TransformerPlugin {
    String id(); // e.g. "NOOP", "UPPER"
    Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}
