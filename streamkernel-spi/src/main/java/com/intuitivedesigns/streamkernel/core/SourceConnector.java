/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A pluggable source of events for the pipeline.
 *
 * @param <T> Raw data type produced by this source.
 */
public interface SourceConnector<T> {

    void connect();

    void disconnect();

    PipelinePayload<T> fetch();

    default List<PipelinePayload<T>> fetchBatch(int maxBatchSize) {
        if (maxBatchSize <= 0) return Collections.emptyList();

        // Fast path for single fetch (avoids loop + extra logic in common cases)
        if (maxBatchSize == 1) {
            PipelinePayload<T> one = fetch();
            return (one == null) ? Collections.emptyList() : Collections.singletonList(one);
        }

        PipelinePayload<T> first = fetch();
        if (first == null) return Collections.emptyList();

        // Pre-size exactly; ArrayList won't allocate more than needed for fill.
        List<PipelinePayload<T>> batch = new ArrayList<>(maxBatchSize);
        batch.add(first);

        for (int i = 1; i < maxBatchSize; i++) {
            PipelinePayload<T> next = fetch();
            if (next == null) break;
            batch.add(next);
        }

        // Avoid leaking capacity when caller stores batches long-term
        return (batch.size() == maxBatchSize) ? batch : Collections.unmodifiableList(batch);
    }
}
