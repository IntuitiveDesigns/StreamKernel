package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.bench.SyntheticAvroSource;
import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.SourcePlugin;

public class SyntheticAvroPlugin implements SourcePlugin {

    @Override
    public String id() {
        return "SYNTHETIC_AVRO";
    }

    @Override
    public SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) {
        // You could add config parsing here if you wanted configurable payload size
        return new SyntheticAvroSource();
    }
}