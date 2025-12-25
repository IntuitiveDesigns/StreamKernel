/*
 * Copyright 2025 Steven Lopez
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package com.example.streamkernel.kafka.core;

/**
 * A stateless transformation step in the pipeline.
 *
 * Examples:
 * - JSON → Domain Object mapping
 * - PII masking or redaction
 * - Enrichment (calling an external service or cache)
 * - Routing decisions (e.g., tag events for different sinks)
 *
 * The Transformer sits between a {@link SourceConnector} and an {@link OutputSink},
 * and operates on {@link PipelinePayload} envelopes to preserve traceability.
 *
 * @param <I> Input data type
 * @param <O> Output data type
 */
public interface Transformer<I, O> {

    /**
     * Transform the input payload into a new payload.
     *
     * <p><b>Traceability Contract:</b></p>
     * <ul>
     *     <li>Implementations should preserve the original ID, timestamp,
     *         and metadata for correct end-to-end tracing.</li>
     *     <li>Use {@link PipelinePayload#withData(Object)} to change the data
     *         while keeping the envelope intact.</li>
     * </ul>
     *
     * <p><b>Failure Contract:</b></p>
     * <ul>
     *     <li>Throw an exception if the transformation fails (e.g., invalid payload, schema error).</li>
     *     <li>The Orchestrator will catch the exception and route the original input payload
     *         to the DLQ sink.</li>
     * </ul>
     *
     * @param input the incoming payload
     * @return a new payload with transformed data (and preserved ID/timestamp/metadata)
     * @throws Exception if the transformation cannot be completed
     */
    PipelinePayload<O> transform(PipelinePayload<I> input) throws Exception;
}
