# 08 — MongoDB Vector Sink

Validates writing to MongoDB collection.

## 1) Start mongodb

```powershell
docker compose up -d mongodb
```

## 2) Configure StreamKernel

```properties
sink.type=MONGODB
mongodb.uri=mongodb://localhost:27017
mongodb.database=support_db
mongodb.collection=tickets_vectorized
```

## 3) Run StreamKernel

```powershell
java -Xms4g -Xmx4g -XX:+UseZGC -XX:+ZGenerational -jar .\build\libs\StreamKernel-0.0.1-SNAPSHOT-all.jar
```

## 4) Verify

```powershell
docker exec -it arena-mongodb mongosh
```

```javascript
use support_db
db.tickets_vectorized.countDocuments()
db.tickets_vectorized.findOne()
```
