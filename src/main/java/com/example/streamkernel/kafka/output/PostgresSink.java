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

package com.example.streamkernel.kafka.output;

import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.PipelinePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple JDBC-based sink that batches inserts into a Postgres table.
 *
 * Table schema example:
 *
 *  CREATE TABLE pipeline_events (
 *      id          VARCHAR(64) PRIMARY KEY,
 *      payload     TEXT NOT NULL,
 *      created_at  TIMESTAMP DEFAULT NOW()
 *  );
 */
public class PostgresSink implements OutputSink<String> {

    private static final Logger log = LoggerFactory.getLogger(PostgresSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int batchSize;

    private Connection connection;
    private PreparedStatement statement;
    private int pending = 0;

    private final List<PipelinePayload<String>> buffer = new ArrayList<>();

    public PostgresSink(String jdbcUrl,
                        String username,
                        String password,
                        int batchSize) throws SQLException {

        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;

        init();
    }

    private void init() throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        this.connection.setAutoCommit(false);

        this.statement = connection.prepareStatement(
                "INSERT INTO pipeline_events (id, payload, created_at) VALUES (?::uuid, ?, ?) " +
                        "ON CONFLICT (id) DO NOTHING"
        );

        log.info("PostgresSink connected to {}", jdbcUrl);
    }

    @Override
    public synchronized void write(PipelinePayload<String> payload) throws Exception {
        buffer.add(payload);
        bindPayload(payload);
        statement.addBatch();
        pending++;

        if (pending >= batchSize) {
            flush();
        }
    }

    private void bindPayload(PipelinePayload<String> payload) throws SQLException {
        statement.setString(1, payload.id());
        statement.setString(2, payload.data());
        statement.setTimestamp(
                3,
                Timestamp.from(payload.timestamp().atZone(ZoneOffset.UTC).toInstant())
        );
    }

    private void flush() throws SQLException {
        if (pending == 0) return;

        int[] results = statement.executeBatch();
        connection.commit();
        log.debug("PostgresSink flushed {} events ({} results)", pending, results.length);

        buffer.clear();
        pending = 0;
    }

    public synchronized void close() throws Exception {
        try {
            flush();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
            log.info("PostgresSink closed");
        }
    }
}
