package com.example.streamkernel.kafka.output;

import com.example.streamkernel.avro.EnrichedTicket; // Generated Avro class
import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoVectorSink implements OutputSink<EnrichedTicket> {

    private static final Logger log = LoggerFactory.getLogger(MongoVectorSink.class);

    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;
    private final MetricsRuntime metrics;

    // Private constructor (Factory Pattern)
    private MongoVectorSink(String uri, String dbName, String colName, MetricsRuntime metrics) {
        this.mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(colName);
        this.metrics = metrics;

        log.info("✅ Connected to MongoDB: {}/{}", dbName, colName);
    }

    // Static Factory
    public static MongoVectorSink fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        return new MongoVectorSink(
                config.getProperty("mongodb.uri", "mongodb://localhost:27017"),
                config.getProperty("mongodb.database", "support_db"),
                config.getProperty("mongodb.collection", "tickets_vectorized"),
                metrics
        );
    }

    @Override
    public void write(PipelinePayload<EnrichedTicket> payload) {
        EnrichedTicket data = payload.data();

        // 1. Convert Avro to BSON Document
        // We explicitly map the vector field so MongoDB recognizes it for Atlas Vector Search
        Document doc = new Document("ticketId", data.getTicketId())
                .append("description", data.getDescription())
                .append("sentiment", data.getSentiment())
                .append("vector_embedding", data.getEmbedding()) // Stored as Array<Float>
                .append("_last_updated", System.currentTimeMillis());

        // 2. Upsert (Replace if exists, Insert if new) based on Ticket ID
        collection.replaceOne(
                new Document("ticketId", data.getTicketId()),
                doc,
                new ReplaceOptions().upsert(true)
        );

        // Optional: Track metric
        // metrics.counter("sink.mongo.writes").increment();
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}