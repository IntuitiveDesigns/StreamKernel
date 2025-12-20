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