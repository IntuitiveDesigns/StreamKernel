package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.bench.SyntheticSource;
import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.SourcePlugin;

public final class SyntheticSourcePlugin implements SourcePlugin {

    @Override
    public String id() {
        return "SYNTHETIC";
    }

    @Override
    public SourceConnector<String> create(PipelineConfig config, MetricsRuntime metrics) {
        int payloadSize = Integer.parseInt(config.getProperty("source.synthetic.payload.size", "1024"));
        boolean highEntropy = Boolean.parseBoolean(config.getProperty("source.synthetic.high.entropy", "false"));
        return new SyntheticSource(payloadSize, highEntropy);
    }
}
