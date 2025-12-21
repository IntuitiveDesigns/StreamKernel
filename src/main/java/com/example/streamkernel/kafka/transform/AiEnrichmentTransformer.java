package com.example.streamkernel.kafka.transform;

import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.core.Transformer;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.avro.CustomerEvent;  // Generated Avro (Input)
import com.example.streamkernel.avro.EnrichedTicket; // Generated Avro (Output)

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AiEnrichmentTransformer implements Transformer<CustomerEvent, EnrichedTicket> {

    private final boolean useMock;
    private final MetricsRuntime metrics;
    private final Random random = new Random();

    public AiEnrichmentTransformer(boolean useMock, MetricsRuntime metrics) {
        this.useMock = useMock;
        this.metrics = metrics;
    }

    @Override
    public PipelinePayload<EnrichedTicket> transform(PipelinePayload<CustomerEvent> input) {
        // 1. DEFINE RAW DATA (This was missing in your paste)
        CustomerEvent raw = input.data();

        // 2. DEFINE SENTIMENT & VECTOR (This was missing)
        String sentiment = raw.getName().toLowerCase().contains("fail") ? "NEGATIVE" : "NEUTRAL";
        List<Float> vector = useMock ? mockEmbedding() : callEmbeddingApi(raw.getName());

        // 3. Build Enriched Object
        EnrichedTicket enriched = EnrichedTicket.newBuilder()
                .setTicketId(raw.getCustomerId())
                .setDescription(raw.getName())
                .setSentiment(sentiment)
                .setEmbedding(vector)
                .build();

        // 4. Return new payload
        return new PipelinePayload<>(
                input.id(),
                enriched,
                input.timestamp(),
                input.headers()
        );
    }

    private List<Float> mockEmbedding() {
        // Generate random vector for demo purposes (dimension 5)
        List<Float> v = new ArrayList<>();
        for (int i = 0; i < 5; i++) v.add(random.nextFloat());
        return v;
    }

    private List<Float> callEmbeddingApi(String text) {
        // TODO: Implement HTTP call to OpenAI / Ollama / HuggingFace here
        return new ArrayList<>();
    }
}