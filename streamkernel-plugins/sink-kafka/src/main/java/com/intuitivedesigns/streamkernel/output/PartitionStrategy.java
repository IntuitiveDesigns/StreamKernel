/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.core.PipelinePayload;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Strategy pattern for determining which Kafka Partition to write to.
 */
@FunctionalInterface
public interface PartitionStrategy {

    /**
     * Determine the partition index for a record.
     *
     * @param payload        The data being sent
     * @param partitionCount Total partitions available in the topic (must be > 0)
     * @return The partition index (0 to partitionCount - 1)
     */
    int partition(PipelinePayload<?> payload, int partitionCount);

    // --- FACTORY METHODS ---

    /**
     * Rotates through partitions evenly (0, 1, 2, 0, 1...).
     * Good for load balancing consumers, but breaks ordering guarantees if data is related.
     */
    static PartitionStrategy roundRobin() {
        return new PartitionStrategy() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public int partition(PipelinePayload<?> payload, int count) {
                if (count <= 0) return 0; // Defensive

                // Mask sign bit to ensure positive result, even for Integer.MIN_VALUE
                return (counter.getAndIncrement() & 0x7FFFFFFF) % count;
            }
        };
    }

    /**
     * Hashes the Payload ID to ensure the same ID always goes to the same partition.
     * Critical for ordering (e.g., updates for User 123 must be sequential).
     */
    static PartitionStrategy keyHash() {
        return (payload, count) -> {
            if (count <= 0) return 0;

            String key = payload.id();
            if (key == null) return 0; // Fallback for missing IDs

            // Use Bitwise AND to strip sign bit.
            // This is safer than Math.abs() which fails on Integer.MIN_VALUE.
            return (key.hashCode() & 0x7FFFFFFF) % count;
        };
    }
}