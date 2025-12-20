package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;

public interface SinkPlugin {
    String id(); // e.g. "KAFKA", "DEVNULL", "POSTGRES", "DLQ_LOG"
    OutputSink<String> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}
