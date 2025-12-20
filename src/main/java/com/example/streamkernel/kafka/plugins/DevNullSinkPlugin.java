package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.SinkPlugin;

public final class DevNullSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "DEVNULL";
    }

    @Override
    public OutputSink<String> create(PipelineConfig config, MetricsRuntime metrics) {
        return payload -> { /* intentionally no-op */ };
    }
}
