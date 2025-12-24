package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.config.PipelineFactory;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.output.KafkaDlqBytesSink;
import com.example.streamkernel.kafka.spi.DlqSerializer;
import com.example.streamkernel.kafka.spi.SinkPlugin;

public final class KafkaDlqSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "KAFKA_DLQ";
    }

    @Override
    @SuppressWarnings("unchecked")
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception {
        String topic = config.getProperty("dlq.topic", "streamkernel-dlq");

        DlqSerializer<?> serializer = PipelineFactory.createDlqSerializer(config, metrics);

        // Raw cast is acceptable here because serializer selection is config-driven and DLQ is an edge sink.
        return new KafkaDlqBytesSink<>(topic, KafkaDlqBytesSink.buildProducerProps(config), (DlqSerializer<Object>) serializer);
    }
}
