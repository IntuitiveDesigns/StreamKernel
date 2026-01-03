/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Production-Grade Postgres Sink.
 * Features:
 * - HikariCP Connection Pooling (Resilience)
 * - Dual-Trigger Flushing (Batch Size OR Linger Time)
 * - Metrics Integration
 */
public final class PostgresSink implements OutputSink<String> {

    private static final Logger log = LoggerFactory.getLogger(PostgresSink.class);

    // Config Defaults
    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final long DEFAULT_LINGER_MS = 1000;
    private static final int DEFAULT_POOL_SIZE = 5;

    private final HikariDataSource dataSource;
    private final int batchSize;
    private final MetricsRuntime metrics;

    // Buffering State
    private final List<PipelinePayload<String>> buffer;
    private final ReentrantLock lock = new ReentrantLock();

    // Background Flusher
    private final ScheduledExecutorService scheduler;

    private PostgresSink(HikariDataSource dataSource, int batchSize, long lingerMs, MetricsRuntime metrics) {
        this.dataSource = dataSource;
        this.batchSize = batchSize;
        this.metrics = metrics;
        this.buffer = new ArrayList<>(batchSize);

        // Setup Linger (Time-based flush)
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "postgres-sink-flusher");
            t.setDaemon(true);
            return t;
        });

        if (lingerMs > 0) {
            this.scheduler.scheduleWithFixedDelay(this::flushIfPending, lingerMs, lingerMs, TimeUnit.MILLISECONDS);
        }

        log.info("✅ PostgresSink Active. Batch={}, Linger={}ms", batchSize, lingerMs);
    }

    public static PostgresSink fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");

        // 1. HikariCP Config
        HikariConfig hikari = new HikariConfig();
        String url = config.getString("postgres.url", "jdbc:postgresql://localhost:5432/streamdb");
        if (!url.contains("reWriteBatchedInserts")) {
            // Smart append to ensure high performance
            url += (url.contains("?") ? "&" : "?") + "reWriteBatchedInserts=true";
        }
        hikari.setJdbcUrl(url);
        hikari.setUsername(config.getString("postgres.username", "postgres"));
        hikari.setPassword(config.getString("postgres.password", "password"));

        // Pool Tuning for Sink Throughput
        hikari.setMaximumPoolSize(config.getInt("postgres.pool.size", DEFAULT_POOL_SIZE));
        hikari.setMinimumIdle(1);
        hikari.setConnectionTimeout(5000);
        hikari.setAutoCommit(false); // We control transactions manually

        // Performance Optimization: Cache Prepared Statements driver-side
        hikari.addDataSourceProperty("cachePrepStmts", "true");
        hikari.addDataSourceProperty("prepStmtCacheSize", "250");
        hikari.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        HikariDataSource ds = new HikariDataSource(hikari);

        int batch = config.getInt("postgres.batch.size", DEFAULT_BATCH_SIZE);
        long linger = config.getLong("postgres.linger.ms", DEFAULT_LINGER_MS);

        return new PostgresSink(ds, batch, linger, metrics);
    }

    @Override
    public void write(PipelinePayload<String> payload) {
        if (payload == null) return;

        lock.lock();
        try {
            buffer.add(payload);
            if (buffer.size() >= batchSize) {
                flushInternal();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Called by the background scheduler to ensure low latency.
     */
    private void flushIfPending() {
        if (buffer.isEmpty()) return;

        lock.lock();
        try {
            if (!buffer.isEmpty()) {
                flushInternal();
            }
        } finally {
            lock.unlock();
        }
    }

    private void flushInternal() {
        if (buffer.isEmpty()) return;
        long start = System.nanoTime();
        int count = buffer.size();

        // SQL: Upsert (Idempotent)
        String sql = "INSERT INTO pipeline_events (id, payload, created_at) VALUES (?::uuid, ?, ?) " +
                "ON CONFLICT (id) DO NOTHING";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (PipelinePayload<String> p : buffer) {
                stmt.setString(1, p.id());
                stmt.setString(2, p.data());
                // Safe timestamp conversion
                Timestamp ts = (p.timestamp() != null)
                        ? Timestamp.from(p.timestamp().toInstant())
                        : new Timestamp(System.currentTimeMillis());
                stmt.setTimestamp(3, ts);

                stmt.addBatch();
            }

            stmt.executeBatch();
            conn.commit();

            // Metrics
            metrics.counter("sink.postgres.written", count);
            metrics.timer("sink.postgres.latency", (System.nanoTime() - start) / 1_000_000);

            buffer.clear();

        } catch (SQLException e) {
            log.error("Postgres Batch Failed (size={}): {}", count, e.getMessage());
            metrics.counter("sink.postgres.errors", 1.0);

            // In a real scenario, you might want to retry, or dump 'buffer' to a DLQ here
            // For now, we clear the buffer to prevent the pipeline from getting stuck on bad data
            buffer.clear();
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            // One final flush attempt
            flushIfPending();
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            dataSource.close();
            log.info("PostgresSink Closed.");
        }
    }
}