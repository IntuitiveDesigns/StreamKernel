package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.Transformer;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.TransformerPlugin;
import com.example.streamkernel.kafka.transform.AiEnrichmentTransformer;

public class AiTransformerPlugin implements TransformerPlugin {

    @Override
    public String id() {
        return "AI_ENRICHMENT";
    }

    // CHANGED: Return Transformer<?, ?> to match the new interface signature
    @Override
    public Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        String provider = config.getProperty("ai.provider", "MOCK").toUpperCase();
        boolean useMock = "MOCK".equals(provider);
        return new AiEnrichmentTransformer(useMock, metrics);
    }
}