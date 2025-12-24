# 07 — Schema Registry + Avro

Validates Schema Registry + Avro serialization.

## 1) Confirm Schema Registry

```powershell
docker compose ps
curl.exe -s http://localhost:8081/subjects
```

## 2) Create topic

```powershell
docker exec -it arena-broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists `
  --topic arena-avro-test --partitions 6 --replication-factor 1
```

## 3) Configure StreamKernel

```properties
source.type=SYNTHETIC_AVRO
sink.type=KAFKA_AVRO
sink.topic=arena-avro-test
schema.registry.url=http://localhost:8081
schema.path=schemas/EnrichedTicket.avsc
```

## 4) Run

```powershell
java -Xms4g -Xmx4g -XX:+UseZGC -XX:+ZGenerational -jar .\build\libs\StreamKernel-0.0.1-SNAPSHOT-all.jar
```

## 5) Validate schema registered

```powershell
curl.exe -s http://localhost:8081/subjects
```
