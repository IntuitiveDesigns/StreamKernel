package com.example.streamkernel.kafka.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class MonitoredTransformer<I, O> implements Transformer<I, O> {

    private final Transformer<I, O> delegate;
    private final Timer timer;

    public MonitoredTransformer(Transformer<I, O> delegate, String metricName, MeterRegistry registry) {
        this.delegate = delegate;
        this.timer = registry.timer(metricName);
    }

    @Override
    public PipelinePayload<O> transform(PipelinePayload<I> input) {
        // Wrap the execution in a timer
        return timer.record(() -> {
            try {
                return delegate.transform(input);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}