package com.example.streamkernel.kafka.output;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class KafkaSink implements OutputSink<String>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean syncSend;
    private final String pipelineName;

    // Local counters
    private final LongAdder sentOk = new LongAdder();
    private final LongAdder sentFail = new LongAdder();
    private final LongAdder inFlight = new LongAdder();

    // Backpressure (acked-based)
    private final long maxInFlightRecords;
    private final long inFlightWaitMs;

    // Metrics (Micrometer)
    private final Counter okCounter;
    private final Counter failCounter;
    private final Timer sendLatencyTimer;

    // Rate-limited logging
    private final long errorLogIntervalMs;
    private final AtomicLong lastErrorLogMs = new AtomicLong(0);
    private final LongAdder suppressedErrorLogs = new LongAdder();

    public KafkaSink(
            String topic,
            Properties props,
            boolean syncSend,
            MetricsRuntime metrics,
            String pipelineName
    ) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.syncSend = syncSend;
        this.pipelineName = (pipelineName == null || pipelineName.isBlank()) ? "unknown" : pipelineName;

        // Backpressure defaults (records, not bytes)
        this.maxInFlightRecords = parseLong(props.getProperty("kafka.inflight.max", "500000"), 500000L);
        this.inFlightWaitMs = Math.max(0L, parseLong(props.getProperty("kafka.inflight.wait.ms", "1"), 1L));

        // Logging interval
        this.errorLogIntervalMs = Math.max(100L, parseLong(props.getProperty("streamkernel.sink.error.log.interval.ms", "1000"), 1000L));

        this.producer = new KafkaProducer<>(Objects.requireNonNull(props, "props"));

        MeterRegistry registry = (metrics != null && metrics.enabled()) ? metrics.registry() : null;
        if (registry != null) {
            this.okCounter = registry.counter("streamkernel_kafka_send_ok_total", "pipeline", this.pipelineName, "topic", this.topic);
            this.failCounter = registry.counter("streamkernel_kafka_send_fail_total", "pipeline", this.pipelineName, "topic", this.topic);
            this.sendLatencyTimer = registry.timer("streamkernel_kafka_send_latency", "pipeline", this.pipelineName, "topic", this.topic);
        } else {
            this.okCounter = null;
            this.failCounter = null;
            this.sendLatencyTimer = null;
        }

        log.info("KafkaSink created. topic='{}' syncSend={} pipeline='{}' maxInFlight={} waitMs={}",
                this.topic, this.syncSend, this.pipelineName, this.maxInFlightRecords, this.inFlightWaitMs);
    }

    public KafkaSink(String topic, Properties props, boolean syncSend) {
        this(topic, props, syncSend, null, null);
    }

    public static KafkaSink fromConfig(PipelineConfig config, String topic, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        String pipelineName = config.getProperty("pipeline.name", "StreamKernel");
        boolean syncSend = Boolean.parseBoolean(config.getProperty("kafka.producer.sync", "false"));

        Properties props = buildProducerProps(config);

        // Non-Kafka properties (sink behavior)
        props.putIfAbsent("streamkernel.sink.error.log.interval.ms", config.getProperty("streamkernel.sink.error.log.interval.ms", "1000"));
        props.putIfAbsent("kafka.inflight.max", config.getProperty("kafka.inflight.max", "500000"));
        props.putIfAbsent("kafka.inflight.wait.ms", config.getProperty("kafka.inflight.wait.ms", "1"));

        return new KafkaSink(topic, props, syncSend, metrics, pipelineName);
    }

    private static Properties buildProducerProps(PipelineConfig config) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.broker", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String pipelineName = config.getProperty("pipeline.name", "StreamKernel");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getProperty("kafka.producer.client.id", pipelineName));

        props.put(ProducerConfig.ACKS_CONFIG, config.getProperty("kafka.producer.acks", "1"));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.getProperty("kafka.producer.idempotence", "false"));
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.parseInt(config.getProperty("kafka.producer.retries", "2147483647")));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.parseInt(config.getProperty("kafka.producer.delivery.timeout.ms", "120000")));

        if (config.getProperty("kafka.producer.request.timeout.ms", null) != null) {
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.parseInt(config.getProperty("kafka.producer.request.timeout.ms", "30000")));
        }
        if (config.getProperty("kafka.producer.max.block.ms", null) != null) {
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.parseLong(config.getProperty("kafka.producer.max.block.ms", "60000")));
        }

        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getProperty("kafka.producer.compression", "lz4"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(config.getProperty("kafka.producer.batch.size", "16384")));
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(config.getProperty("kafka.producer.linger.ms", "0")));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.parseLong(config.getProperty("kafka.producer.buffer.memory", "33554432")));
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Integer.parseInt(config.getProperty("kafka.producer.max.request.size", "1048576")));

        String inFlight = config.getProperty("kafka.producer.max.in.flight.requests.per.connection", null);
        if (inFlight == null) inFlight = config.getProperty("kafka.producer.max.in.flight", "5");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.parseInt(inFlight));

        return props;
    }

    @Override
    public void write(PipelinePayload<String> payload) throws Exception {
        // Ack-based backpressure gate
        applyBackpressureIfNeeded();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload.id(), payload.data());

        if (syncSend) {
            long start = System.nanoTime();
            inFlight.increment();
            try {
                producer.send(record).get();
                markOk(start);
            } catch (Exception e) {
                markFail(start, payload.id(), e);
                throw e;
            } finally {
                inFlight.decrement();
            }
            return;
        }

        final long start = System.nanoTime();
        inFlight.increment();

        producer.send(record, (metadata, exception) -> {
            try {
                if (exception == null) markOk(start);
                else markFail(start, payload.id(), exception);
            } finally {
                inFlight.decrement();
            }
        });
    }

    private void applyBackpressureIfNeeded() {
        if (maxInFlightRecords <= 0) return;

        // Fast path
        if (inFlight.sum() <= maxInFlightRecords) return;

        // Slow path: throttle until in-flight drops
        while (inFlight.sum() > maxInFlightRecords) {
            if (inFlightWaitMs <= 0) {
                Thread.yield();
            } else {
                try { Thread.sleep(inFlightWaitMs); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    private void markOk(long startNanos) {
        sentOk.increment();
        if (okCounter != null) okCounter.increment();
        if (sendLatencyTimer != null) sendLatencyTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    private void markFail(long startNanos, String key, Throwable exception) {
        sentFail.increment();
        if (failCounter != null) failCounter.increment();
        if (sendLatencyTimer != null) sendLatencyTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);

        long now = System.currentTimeMillis();
        long last = lastErrorLogMs.get();

        if (now - last >= errorLogIntervalMs && lastErrorLogMs.compareAndSet(last, now)) {
            long suppressed = suppressedErrorLogs.sumThenReset();
            if (suppressed > 0) {
                log.warn("Kafka send failures ongoing. suppressedLogs={} topic={} pipeline={}",
                        suppressed, topic, pipelineName);
            }
            log.error("Kafka async send failed. key={} topic={} pipeline={} ex={}",
                    key, topic, pipelineName, exception.getClass().getSimpleName(), exception);
        } else {
            suppressedErrorLogs.increment();
        }
    }

    private static long parseLong(String value, long fallback) {
        if (value == null) return fallback;
        try { return Long.parseLong(value.trim()); }
        catch (Exception e) { return fallback; }
    }

    public long sentOkTotal() { return sentOk.sum(); }
    public long sentFailTotal() { return sentFail.sum(); }
    public long inFlightTotal() { return inFlight.sum(); }

    @Override
    public void close() {
        try { producer.flush(); }
        catch (Exception e) { log.warn("Producer flush failed (continuing close). topic={} pipeline={}", topic, pipelineName, e); }

        try { producer.close(Duration.ofSeconds(10)); }
        catch (Exception e) { log.warn("Producer close failed. topic={} pipeline={}", topic, pipelineName, e); }

        log.info("KafkaSink closed. topic='{}' pipeline='{}' ok={} fail={} inflight={}",
                topic, pipelineName, sentOk.sum(), sentFail.sum(), inFlight.sum());
    }
}
