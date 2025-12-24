# 06 — Transformers

Validates transformer selection.

## NOOP

```properties
transform.type=NOOP
```

## UPPER

```properties
transform.type=UPPER
```

Validate by consuming from the sink topic:

```powershell
docker exec -it arena-broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```
