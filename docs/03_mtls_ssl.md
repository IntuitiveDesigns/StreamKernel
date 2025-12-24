# 03 — mTLS (SSL) Kafka Connectivity

Validates:
- Broker listens on 9093
- Client keystore/truststore readable
- StreamKernel produces via SSL

## 1) Ensure broker starts cleanly

Broker requires `KAFKA_SSL_KEY_CREDENTIALS` and friends. Create this file:

```powershell
"changeit" | Out-File -Encoding ascii .\secrets\password.txt
```

Restart broker:
```powershell
docker compose up -d --force-recreate broker
docker logs arena-broker --tail 80
```

## 2) Verify port

```powershell
Test-NetConnection localhost -Port 9093
```

## 3) Verify keystores

```powershell
keytool -list -storetype PKCS12 -keystore .\secrets\kafka.client.keystore.p12 -storepass changeit
keytool -list -storetype PKCS12 -keystore .\secrets\kafka.truststore.p12 -storepass changeit
```

## 4) Configure StreamKernel

```properties
kafka.broker=localhost:9093
kafka.security.protocol=SSL

kafka.ssl.keystore.location=C:/workspace/StreamKernel/secrets/kafka.client.keystore.p12
kafka.ssl.truststore.location=C:/workspace/StreamKernel/secrets/kafka.truststore.p12
kafka.ssl.keystore.type=PKCS12
kafka.ssl.truststore.type=PKCS12
kafka.ssl.keystore.password=changeit
kafka.ssl.truststore.password=changeit
kafka.ssl.key.password=changeit

# For local CN=localhost dev
kafka.ssl.endpoint.identification.algorithm=
```

## 5) Run + Validate

Run StreamKernel, then consume from primary (PLAINTEXT consumer inside docker):

```powershell
docker exec -it arena-broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```
