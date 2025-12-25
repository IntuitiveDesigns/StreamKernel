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
package com.example.streamkernel.kafka;

import com.example.streamkernel.kafka.bench.SyntheticSource;
import com.example.streamkernel.kafka.core.SourceConnector;

public class SourceBurnTest {
    public static void main(String[] args) {
        // 1. Setup High-Entropy Source (Random Data)
        // 1024 bytes, True = Random
        SourceConnector<String> source = new SyntheticSource(1024);

        long start = System.currentTimeMillis();
        long count = 0;

        System.out.println("🔥 Starting RAM Burn Test...");

        while (true) {
            // Fetch 4000 items (The "Truck" size)
            var batch = source.fetchBatch(4000);
            count += batch.size();

            if (count % 10_000_000 == 0) {
                long now = System.currentTimeMillis();
                double seconds = (now - start) / 1000.0;
                System.out.printf("SPEED: %,.0f Events/Sec%n", count / seconds);
            }
        }
    }
}