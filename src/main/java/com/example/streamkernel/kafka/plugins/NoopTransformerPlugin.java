package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.Transformer;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.TransformerPlugin;

public final class NoopTransformerPlugin implements TransformerPlugin {

    @Override
    public String id() {
        return "NOOP";
    }

    @Override
    public Transformer<String, String> create(PipelineConfig config, MetricsRuntime metrics) {
        return input -> input;
    }
}
