package com.example.streamkernel.kafka.bench;

import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.PipelinePayload;

/**
 * No-op sink used for SOURCE benchmark scenarios.
 * Lets you measure source + transform throughput without downstream I/O.
 */
public final class DevNullSink<T> implements OutputSink<T> {

    @Override
    public void write(PipelinePayload<T> payload) {
        // intentionally no-op
    }
}
