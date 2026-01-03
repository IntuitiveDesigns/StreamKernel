/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.core;

/**
 * A transformation step in the pipeline.
 *
 * Capabilities:
 * - 1-to-1 Mapping: Transform data types (JSON -> Object).
 * - Filtering: Return {@code null} to drop the event from the stream.
 * - Enrichment: Call external APIs or Caches (using {@link #init()} and {@link #close()}).
 *
 * @param <I> Input data type
 * @param <O> Output data type
 */
public interface Transformer<I, O> extends AutoCloseable {

    /**
     * Initialize resources (optional).
     * Called once when the pipeline starts.
     * Use this to set up HTTP clients, database connections, or load ML models.
     */
    default void init() throws Exception {
        // no-op by default
    }

    /**
     * Transform the input payload.
     *
     * <p><b>Contract:</b></p>
     * <ul>
     * <li><b>Traceability:</b> Use {@code input.withData(newData)} to ensure IDs and Timestamps are preserved.</li>
     * <li><b>Filtering:</b> Return {@code null} to drop this event. The Orchestrator will skip downstream steps.</li>
     * <li><b>Failure:</b> Throwing an exception triggers the DLQ flow.</li>
     * </ul>
     *
     * @param input the incoming payload
     * @return the transformed payload, or {@code null} to filter it out.
     * @throws Exception if a non-recoverable error occurs.
     */
    PipelinePayload<O> transform(PipelinePayload<I> input) throws Exception;

    /**
     * Clean up resources (optional).
     * Called once when the pipeline stops.
     */
    @Override
    default void close() throws Exception {
        // no-op by default
    }
}