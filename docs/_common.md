# Common Prereqs & Conventions

## Assumptions

- StreamKernel runs on the host (Windows PowerShell).
- Infra runs via Docker Compose in the repo root.
- Container names match your compose file (e.g., `arena-broker`, `streamkernel-opa-1`).

## Common Ports

- Kafka PLAINTEXT (host): `localhost:9092`
- Kafka SSL/mTLS (host): `localhost:9093`
- Kafka inside Docker: `broker:29092`
- OPA API: `http://localhost:8181`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Schema Registry: `http://localhost:8081`
- MongoDB: `mongodb://localhost:27017`

## Verify Containers

```powershell
docker compose ps
docker logs arena-broker --tail 50
docker logs streamkernel-opa-1 --tail 50
```

## Topic Commands

List topics:
```powershell
docker exec -it arena-broker kafka-topics --bootstrap-server broker:29092 --list
```

Describe a topic:
```powershell
docker exec -it arena-broker kafka-topics --bootstrap-server broker:29092 --describe --topic arena-bench-test
```

Consume from a topic:
```powershell
docker exec -it arena-broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```

## PowerShell curl

Use `curl.exe` for consistent behavior:

```powershell
@'
{"input":{"user":"service-account-1","action":"write","resource":"arena-bench-test"}}
'@ | Out-File -Encoding ascii .\opa_input.json

curl.exe -s -X POST "http://localhost:8181/v1/data/streamkernel/authz/allow" `
  -H "Content-Type: application/json" `
  --data-binary "@opa_input.json"
```

## Secrets Handling

Do not commit private keys/keystores. Add to `.gitignore`:
- `secrets/`
- `*.jks`
- `*.p12`
- `password.txt`
