/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.avro.EnrichedTicket;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * MongoDB Sink optimized for Vector Upserts.
 * Compatible with Virtual Threads (uses synchronous driver).
 */
public final class MongoVectorSink implements OutputSink<EnrichedTicket> {

    private static final Logger log = LoggerFactory.getLogger(MongoVectorSink.class);

    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;
    private final MetricsRuntime metrics;

    // Reusable options (allocating these per-write is wasteful)
    private static final ReplaceOptions UPSERT_OPTIONS = new ReplaceOptions().upsert(true);

    private MongoVectorSink(MongoClient client, MongoCollection<Document> col, MetricsRuntime metrics) {
        this.mongoClient = client;
        this.collection = col;
        this.metrics = metrics;
    }

    public static MongoVectorSink fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        String uriStr = config.getString("mongodb.uri", "mongodb://localhost:27017");
        String dbName = config.getString("mongodb.database", "support_db");
        String colName = config.getString("mongodb.collection", "tickets_vectorized");

        // Optimize Connection Pool for Streaming
        // (Allows high concurrency from Virtual Threads without blocking on pool exhaustion)
        ConnectionString connString = new ConnectionString(uriStr);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .applyToConnectionPoolSettings(builder ->
                        builder.maxSize(100) // Default is 100, ensure it matches your thread parallelism
                                .maxWaitTime(2_000, TimeUnit.MILLISECONDS))
                .build();

        MongoClient client = MongoClients.create(settings);
        MongoDatabase db = client.getDatabase(dbName);
        MongoCollection<Document> col = db.getCollection(colName);

        log.info("✅ MongoDB Sink Active: {}/{}", dbName, colName);

        return new MongoVectorSink(client, col, metrics);
    }

    @Override
    public void write(PipelinePayload<EnrichedTicket> payload) {
        if (payload == null || payload.data() == null) return;

        EnrichedTicket data = payload.data();
        long start = System.nanoTime();

        try {
            // 1. Map to BSON
            // Note: MongoDB handles List<Float> -> Array<Double> conversions automatically for vectors
            Document doc = new Document()
                    .append("ticketId", data.getTicketId())
                    .append("description", data.getDescription())
                    .append("sentiment", data.getSentiment())
                    .append("vector_embedding", data.getEmbedding())
                    .append("_last_updated", System.currentTimeMillis());

            // 2. Upsert (Idempotent)
            // Filter by Business Key (ticketId), not the BSON _id, to allow external IDs
            collection.replaceOne(
                    new Document("ticketId", data.getTicketId()),
                    doc,
                    UPSERT_OPTIONS
            );

            // 3. Success Metrics
            metrics.timer("sink.mongo.latency", (System.nanoTime() - start) / 1_000_000);
            metrics.counter("sink.mongo.writes.ok", 1.0);

        } catch (Exception e) {
            // 4. Failure Handling
            metrics.counter("sink.mongo.writes.fail", 1.0);
            log.error("Mongo Write Failed for Ticket ID: {}", data.getTicketId(), e);

            // Re-throw to trigger pipeline DLQ logic
            throw new RuntimeException("MongoDB Upsert Failed", e);
        }
    }

    @Override
    public void close() {
        log.info("Closing MongoDB connection...");
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}