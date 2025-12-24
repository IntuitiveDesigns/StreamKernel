package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;

/**
 * ServiceLoader entry point for DLQ serializers.
 */
public interface DlqSerializerPlugin {
    String id();
    DlqSerializer<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}
