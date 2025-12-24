package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.serializers.StringDlqSerializer;
import com.example.streamkernel.kafka.spi.DlqSerializer;
import com.example.streamkernel.kafka.spi.DlqSerializerPlugin;

public final class StringDlqSerializerPlugin implements DlqSerializerPlugin {

    @Override
    public String id() {
        return "STRING";
    }

    @Override
    public DlqSerializer<?> create(PipelineConfig config, MetricsRuntime metrics) {
        return new StringDlqSerializer<>();
    }
}
