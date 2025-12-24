# Profiles

Profiles are drop-in `.properties` configurations under `config/profiles/`.

## How to select a profile

Recommended patterns (implement any one):

1) **System property** (cleanest):
```bash
java -Dstreamkernel.profile=bench-plaintext -jar build/libs/StreamKernel-0.0.1-SNAPSHOT-all.jar
```

2) **CLI flag**:
```bash
java -jar build/libs/StreamKernel-0.0.1-SNAPSHOT-all.jar --profile=bench-plaintext
```

3) **Env var**:
```bash
set STREAMKERNEL_PROFILE=bench-plaintext
java -jar build/libs/StreamKernel-0.0.1-SNAPSHOT-all.jar
```

## Naming convention

Use intent-first names:
- `bench-*` for max throughput
- `secure-*` for security posture
- `*-schema-registry`, `*-mongodb-*` for integration flows
- `metrics-*` for observability switches
