package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.output.MongoVectorSink;
import com.example.streamkernel.kafka.spi.SinkPlugin;

public class MongoSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "MONGODB";
    }

    @Override
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) {
        // Delegate to the static factory in the Sink class
        return MongoVectorSink.fromConfig(config, metrics);
    }
}