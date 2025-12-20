package com.example.streamkernel.kafka.output;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class KafkaAvroSink implements OutputSink<String> {

    private static final Logger log = LoggerFactory.getLogger(KafkaAvroSink.class);

    private final KafkaProducer<String, GenericRecord> producer;
    private final String topic;
    private final Schema schema;
    private final MetricsRuntime metrics;

    // Private constructor - forces use of fromConfig()
    private KafkaAvroSink(String topic, Properties props, Schema schema, MetricsRuntime metrics) {
        this.topic = topic;
        this.schema = schema;
        this.metrics = metrics;
        this.producer = new KafkaProducer<>(props);

        log.info("KafkaAvroSink initialized. Topic='{}'", topic);
    }

    // --- STATIC FACTORY (Matches your KafkaSink pattern) ---
    public static KafkaAvroSink fromConfig(PipelineConfig config, String topic, MetricsRuntime metrics) {
        // 1. Validations
        String registryUrl = config.getProperty("schema.registry.url");
        String schemaPath = config.getProperty("schema.path");

        if (registryUrl == null) throw new IllegalArgumentException("Missing 'schema.registry.url' in config");
        if (schemaPath == null) throw new IllegalArgumentException("Missing 'schema.path' in config");

        // 2. Load Schema
        Schema schema;
        try {
            schema = loadSchemaFromResource(schemaPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Avro schema from: " + schemaPath, e);
        }

        // 3. Build Properties
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getProperty("bootstrap.servers", "localhost:9092"));
        props.put("schema.registry.url", registryUrl);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());

        // 4. Return Instance
        return new KafkaAvroSink(topic, props, schema, metrics);
    }

    @Override
    public void write(PipelinePayload<String> payload) {
        try {
            GenericRecord record = new GenericData.Record(schema);
            record.put("customerId", payload.id());
            record.put("name", payload.data());
            record.put("tier", "STANDARD");

            producer.send(new ProducerRecord<>(topic, payload.id(), record));

            // Optional: Record metrics if you have a counter
            // metrics.getCounter("sink.messages.written").increment();

        } catch (Exception e) {
            // Rethrow or handle based on your error policy
            throw new RuntimeException("Failed to write Avro record", e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    private static Schema loadSchemaFromResource(String resourcePath) throws Exception {
        try (InputStream in = KafkaAvroSink.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) throw new IllegalArgumentException("Resource not found: " + resourcePath);
            return new Schema.Parser().parse(in);
        }
    }
}