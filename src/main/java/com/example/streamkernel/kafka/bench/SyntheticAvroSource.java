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
package com.example.streamkernel.kafka.bench;

import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.avro.CustomerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class SyntheticAvroSource implements SourceConnector<CustomerEvent> {
    private static final Logger log = LoggerFactory.getLogger(SyntheticAvroSource.class);

    private static final String[] DESCRIPTIONS = {
            "My screen is flickering red and failing",
            "I love this product, it works perfectly",
            "System crash when loading data",
            "Best purchase ever, highly recommended"
    };

    private final AtomicLong seq = new AtomicLong(0);

    @Override
    public void connect() {
        log.info("✅ Synthetic Avro Source Connected.");
    }

    @Override
    public void disconnect() { }

    @Override
    public PipelinePayload<CustomerEvent> fetch() {
        // single-event call: keep it simple
        return generateOne(Instant.now());
    }

    @Override
    public List<PipelinePayload<CustomerEvent>> fetchBatch(int batchSize) {
        // Single timestamp per batch (reduces clock overhead, improves repeatability)
        Instant now = Instant.now();

        ArrayList<PipelinePayload<CustomerEvent>> out = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            out.add(generateOne(now));
        }
        return out;
    }

    private PipelinePayload<CustomerEvent> generateOne(Instant ts) {
        long n = seq.incrementAndGet();
        String id = "c-" + n;

        ThreadLocalRandom r = ThreadLocalRandom.current();
        String desc = DESCRIPTIONS[r.nextInt(DESCRIPTIONS.length)];

        CustomerEvent event = CustomerEvent.newBuilder()
                .setCustomerId(id)
                .setName(desc)
                .setTier(r.nextBoolean() ? "PLATINUM" : "STANDARD")
                .build();

        return new PipelinePayload<>(id, event, ts, Collections.emptyMap());
    }
}
