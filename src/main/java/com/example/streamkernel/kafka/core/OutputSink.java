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

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.output.KafkaSink;
import com.example.streamkernel.kafka.output.PostgresSink;

import java.sql.SQLException;
import java.util.Properties;

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
 *     <li>{@link #write(PipelinePayload)} should persist or transmit the event.</li>
 *     <li>Implementations may throw exceptions to indicate failures.</li>
 *     <li>The Orchestrator will catch exceptions and route failed events to the DLQ sink.</li>
 *     <li>Implementations should be thread-safe, because many virtual threads
 *         may invoke this method concurrently.</li>
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
     *     <li>Throw an exception if the event cannot be written.</li>
     *     <li>The Orchestrator will route the original event to the DLQ sink automatically.</li>
     *     <li>Do NOT swallow exceptions silently—this hides real operational issues.</li>
     * </ul>
     *
     * @param payload the standardized PipelinePayload envelope
     * @throws Exception if persistence fails or the target system rejects the event
     */
    void write(PipelinePayload<T> payload) throws Exception;

    @Override
    default void close() throws Exception {
        // no-op by default
    }
}
