package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.output.KafkaSink;
import com.example.streamkernel.kafka.spi.SinkPlugin;

public final class KafkaSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "KAFKA";
    }

    @Override
    public OutputSink<String> create(PipelineConfig config, MetricsRuntime metrics) {
        String topic = config.getProperty("sink.topic", "streamkernel-bench-test");
        return KafkaSink.fromConfig(config, topic, metrics);
    }
}
