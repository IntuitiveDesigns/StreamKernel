package com.example.streamkernel.kafka.serializers;

import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.spi.DlqSerializer;

import java.nio.charset.StandardCharsets;

public final class StringDlqSerializer<I> implements DlqSerializer<I> {

    @Override
    public String id() {
        return "STRING";
    }

    @Override
    public byte[] key(PipelinePayload<I> payload) {
        return payload.id().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] value(PipelinePayload<I> payload) {
        // Minimal dependency approach. If payload.data() is already String, this is zero-copy at the object level,
        // but still creates UTF-8 bytes (required for Kafka byte[] serializer).
        return String.valueOf(payload.data()).getBytes(StandardCharsets.UTF_8);
    }
}
