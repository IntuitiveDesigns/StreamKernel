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
package com.example.streamkernel.kafka.output;

import com.example.streamkernel.kafka.core.PipelinePayload;

/**
 * Strategy pattern for determining which Kafka Partition to write to.
 */
@FunctionalInterface
public interface PartitionStrategy {

    /**
     * @param payload The data being sent
     * @param partitionCount Total partitions available in the topic
     * @return The partition index (0 to partitionCount - 1)
     */
    int partition(PipelinePayload<?> payload, int partitionCount);

    // --- FACTORY METHODS FOR COMMON STRATEGIES ---

    // 1. Round Robin (Load Balancing)
    static PartitionStrategy roundRobin() {
        return new PartitionStrategy() {
            private final java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(0);
            @Override
            public int partition(PipelinePayload<?> payload, int count) {
                return Math.abs(counter.getAndIncrement() % count);
            }
        };
    }

    // 2. Key Hash (Data Locality - same ID goes to same partition)
    static PartitionStrategy keyHash() {
        return (payload, count) -> Math.abs(payload.id().hashCode() % count);
    }
}