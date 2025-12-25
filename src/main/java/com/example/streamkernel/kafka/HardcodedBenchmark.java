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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */


package com.example.streamkernel.kafka;

import com.example.streamkernel.kafka.bench.SyntheticSource;
import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.core.SourceConnector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class HardcodedBenchmark {

    public static void main(String[] args) {
        System.out.println("=== HARDCODED CONFIG TEST ===");

        // 1. HARDCODED PRODUCER CONFIG (The "300k" Settings)
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // --- PERFORMANCE TUNING ---
        props.put(ProducerConfig.ACKS_CONFIG, "all");             // Strict
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 4194304);     // 4MB Batch
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);           // 50ms Wait
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // Compression
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 268435456);// 256MB Buffer
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760); // 10MB Safety Valve

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 2. SOURCE (Ring Buffer)
        SourceConnector<String> source = new SyntheticSource(1024);

        // 3. THE LOOP (Bypassing Orchestrator)
        int appBatchSize = 4000; // Match the 4MB Batch
        int threads = 50;        // 50 Trucks

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Semaphore semaphore = new Semaphore(threads);
        AtomicLong counter = new AtomicLong(0);
        long start = System.currentTimeMillis();

        System.out.println("🚀 Starting Traffic...");

        // Reporter Thread
        new Thread(() -> {
            while(true) {
                try { Thread.sleep(1000); } catch (Exception e) {}
                long count = counter.get();
                long now = System.currentTimeMillis();
                System.out.printf("SPEED: %,.0f Events/Sec%n", (count * 1000.0) / (now - start));
            }
        }).start();

        // Main Dispatch Loop
        while (true) {
            var batch = source.fetchBatch(appBatchSize);

            try {
                semaphore.acquire(); // Flow Control
                executor.submit(() -> {
                    try {
                        for (PipelinePayload<String> record : batch) {
                            producer.send(new ProducerRecord<>("streamkernel-bench-test", record.id(), record.data()));
                        }
                        counter.addAndGet(batch.size());
                    } finally {
                        semaphore.release();
                    }
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}