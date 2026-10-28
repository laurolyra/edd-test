# Message broker

Kafka runs in KRaft mode (no ZooKeeper) as a single broker for local study.

## Files

| File | Purpose |
|---|---|
| `topics.py` | Source of truth for topic names, partition counts and retention. |
| `topics.yml` | Human-readable catalogue of producers/consumers per topic. |
| `create_topics.py` | Idempotent topic creation script used by `make topics`, `make dev`, and the Docker `kafka-init` job. |

## Listeners

The Compose broker exposes two listeners:

- `localhost:9092` — for services running on your machine;
- `kafka:29092` — for containers on the Compose network.

This is why the same broker works both with `make dev` and with the Docker `apps` profile.

## Commands

```bash
# Create all topics (safe to run repeatedly)
make topics

# List topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Inspect a topic (partitions, replicas, retention)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders.events

# Inspect a consumer group and its lag
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-service

# Read events from the beginning
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.events \
  --from-beginning --property print.key=true
```

Kafka UI: <http://localhost:8080>.

## Production notes

Local settings are deliberately not production settings:

| Local | Production |
|---|---|
| 1 broker, replication factor 1 | 3+ brokers, replication factor 3 |
| `min.insync.replicas` default | `min.insync.replicas=2`, `acks=all` |
| PLAINTEXT | TLS/SASL, per-topic ACLs |
| 6 partitions | Size from throughput and consumer parallelism |
| 7-day retention | Retention driven by replay/audit requirements |
| Auto-create enabled as a fallback | Auto-create disabled; topics managed declaratively |

Topics are append-only logs. Changing the partition count changes the key → partition mapping, so plan partition counts before relying on per-key ordering at scale.
