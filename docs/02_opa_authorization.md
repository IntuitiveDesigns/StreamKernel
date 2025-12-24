# 02 — OPA Authorization (Topic-Level)

Validates:
- StreamKernel calls OPA
- Allow/deny behavior
- Denied batches route to DLQ

## 1) Confirm OPA Running

```powershell
docker compose ps
docker logs streamkernel-opa-1 --tail 50
```

## 2) Smoke Test OPA

### Allow
```powershell
@'
{"input":{"user":"service-account-1","action":"write","resource":"arena-bench-test"}}
'@ | Out-File -Encoding ascii .\opa_allow.json

curl.exe -s -X POST "http://localhost:8181/v1/data/streamkernel/authz/allow" `
  -H "Content-Type: application/json" `
  --data-binary "@opa_allow.json"
```

### Deny
```powershell
@'
{"input":{"user":"service-account-1","action":"write","resource":"not-allowed-topic"}}
'@ | Out-File -Encoding ascii .\opa_deny.json

curl.exe -s -X POST "http://localhost:8181/v1/data/streamkernel/authz/allow" `
  -H "Content-Type: application/json" `
  --data-binary "@opa_deny.json"
```

## 3) Configure StreamKernel

```properties
security.opa.url=http://localhost:8181/v1/data/streamkernel/authz/allow
app.service.account=service-account-1
sink.topic=arena-bench-test
dlq.type=KAFKA_DLQ
dlq.topic=streamkernel-dlq
```

## 4) Run + Validate

Run StreamKernel, then:

Primary topic:
```powershell
docker exec -it arena-broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```

DLQ (for denied):
```powershell
docker exec -it arena-broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic streamkernel-dlq --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```
