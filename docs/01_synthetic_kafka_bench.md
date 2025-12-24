# 01 — SYNTHETIC → Kafka Bench Test (Ring Buffer)

Validates:
- `source.type=SYNTHETIC` ring-buffer batches
- High-throughput orchestration
- Kafka sink write path

## 1) Start Infra

```powershell
docker compose up -d
docker compose ps
```

## 2) Create Topics

```powershell
docker exec -it arena-broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists `
  --topic arena-bench-test --partitions 6 --replication-factor 1 --config max.message.bytes=10485760

docker exec -it arena-broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists `
  --topic streamkernel-dlq --partitions 6 --replication-factor 1
```

## 3) Configure `pipeline.properties`

```properties
source.type=SYNTHETIC
source.synthetic.payload.size=1024
source.synthetic.high.entropy=false

sink.type=KAFKA
sink.topic=arena-bench-test

dlq.type=KAFKA_DLQ
dlq.topic=streamkernel-dlq
```

## 4) Build + Run

```powershell
./gradlew clean build -x test
java -Xms4g -Xmx4g -XX:+UseZGC -XX:+ZGenerational -jar .\build\libs\StreamKernel-0.0.1-SNAPSHOT-all.jar
```

## 5) Validate Messages

```powershell
docker exec -it arena-broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```

If OPA denies, validate DLQ instead:

```powershell
docker exec -it arena-broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic streamkernel-dlq --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```
