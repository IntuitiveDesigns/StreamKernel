# 04 — DLQ (Log + Kafka DLQ + Serializer)

Validates:
- DLQ plugin works
- Messages land in DLQ when primary path fails/denied
- Serializer outputs are correct

## 1) Configure Kafka DLQ

```properties
dlq.type=KAFKA_DLQ
dlq.topic=streamkernel-dlq
dlq.serializer.type=STRING
```

Create topic:
```powershell
docker exec -it arena-broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists `
  --topic streamkernel-dlq --partitions 6 --replication-factor 1
```

## 2) Force DLQ

Deny via OPA (change sink topic to something not allowed) OR cause a write failure.

Example:
```properties
sink.topic=not-allowed-topic
```

## 3) Verify DLQ has messages

```powershell
docker exec -it arena-broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic streamkernel-dlq --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```
