# 05 — Prometheus + Grafana

Validates:
- StreamKernel exports Prometheus metrics
- Prometheus scrapes
- Grafana visualizes

## 1) Run infra

```powershell
docker compose up -d
```

## 2) Run StreamKernel with Prometheus

```properties
metrics.enabled=true
metrics.type=PROMETHEUS
metrics.prometheus.port=8080
```

Run:
```powershell
java -Xms4g -Xmx4g -XX:+UseZGC -XX:+ZGenerational -jar .\build\libs\StreamKernel-0.0.1-SNAPSHOT-all.jar
```

## 3) Verify metrics endpoint

```powershell
curl.exe -s http://localhost:8080/metrics | Select-String streamkernel
```

## 4) UIs

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
