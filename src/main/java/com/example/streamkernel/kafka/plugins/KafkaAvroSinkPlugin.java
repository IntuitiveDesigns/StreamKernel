package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.output.KafkaAvroSink;
import com.example.streamkernel.kafka.spi.SinkPlugin;

public final class KafkaAvroSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "KAFKA_AVRO";
    }

    @Override
    public OutputSink<String> create(PipelineConfig config, MetricsRuntime metrics) {
        String topic = config.getProperty("sink.topic", "streamkernel-avro-default");
        // Delegate to the Sink class, matching your KafkaSink style
        return KafkaAvroSink.fromConfig(config, topic, metrics);
    }
}