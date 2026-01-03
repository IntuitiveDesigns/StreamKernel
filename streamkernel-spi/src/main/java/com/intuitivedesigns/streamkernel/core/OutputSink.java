/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.core;

/**
 * A pluggable destination for pipeline events.
 *
 * Examples:
 * - Kafka producer
 * - Redis writer
 * - HTTP POST sink
 * - File system writer
 * - Snowflake / JDBC sink
 *
 * <p><b>Contract:</b></p>
 * <ul>
 * <li>{@link #write(PipelinePayload)} should persist or transmit the event.</li>
 * <li>Implementations may throw exceptions to indicate failures.</li>
 * <li>The Orchestrator will catch exceptions and route failed events to the DLQ sink.</li>
 * <li>Implementations should be thread-safe, because many virtual threads
 * may invoke this method concurrently.</li>
 * </ul>
 *
 * @param <T> The type of the event payload.
 */
public interface OutputSink<T> extends AutoCloseable {

    /**
     * Persist or send the payload to the target system.
     *
     * <p><b>Failure Contract:</b></p>
     * <ul>
     * <li>Throw an exception if the event cannot be written.</li>
     * <li>The Orchestrator will route the original event to the DLQ sink automatically.</li>
     * <li>Do NOT swallow exceptions silently—this hides real operational issues.</li>
     * </ul>
     *
     * @param payload the standardized PipelinePayload envelope
     * @throws Exception if persistence fails or the target system rejects the event
     */
    void write(PipelinePayload<T> payload) throws Exception;

    /**
     * Forces any buffered events to be written to the target system.
     * <p>
     * <b>Usage:</b> The Orchestrator should call this before committing
     * source offsets to ensure "At-Least-Once" delivery semantics.
     *
     * @throws Exception if the flush fails
     */
    default void flush() throws Exception {
        // no-op by default for non-batching sinks (e.g., Redis, Stdout)
    }

    /**
     * Returns a unique identifier for this sink instance.
     * Useful for logging and metrics tagging (e.g., "postgres-primary").
     */
    default String id() {
        return this.getClass().getSimpleName();
    }

    /**
     * Clean up resources (connections, thread pools, etc.).
     */
    @Override
    default void close() throws Exception {
        flush(); // Good practice: try to flush remaining data before closing
    }
}