# Event-Driven Shop

A small e-commerce system built to learn event-driven design by running it.

Five FastAPI services communicate **only through Kafka** — no service calls another service over HTTP. Each service owns its own Postgres database, writes events through a transactional outbox, and consumes idempotently.

> The full architecture explanation, failure analysis and interview notes are in
> **[docs/STUDY_GUIDE.md](docs/STUDY_GUIDE.md)**.

## What runs

| Service | Port | Owns | Consumes | Produces |
|---|---:|---|---|---|
| **orders** | 8001 | orders, order items, saga flags | `stock.events`, `invoices.events`, `fiscal.events` | `orders.events` |
| **stock** | 8002 | inventory, reservations | `orders.events` | `stock.events` |
| **invoices** | 8003 | invoices, cancellation markers | `stock.events`, `orders.events` | `invoices.events` |
| **fiscal** | 8004 | fiscal documents (fake NF-e) | `invoices.events` | `fiscal.events` |
| **recommendations** | 8005 | purchase history + co-purchase read model | `orders.events` | — |

Infrastructure:

| Component | Port |
|---|---:|
| Kafka (KRaft, single broker) | 9092 |
| Kafka UI | 8080 |
| Postgres (one server, one database per service) | 5432 |

## Quickstart

Requirements: Docker Desktop, [uv](https://docs.astral.sh/uv/), `make`.

```bash
# 1. Start Kafka + Postgres + Kafka UI
make up

# 2. Start the five services (Terminal 1). Ctrl-C stops all of them.
make dev

# 3. Run the end-to-end demo (Terminal 2)
make demo
```

`make dev` also installs dependencies and creates the Kafka topics.

The demo:

1. places an order and waits for it to become `COMPLETED`,
2. shows the invoice, fiscal document and stock reservation created asynchronously,
3. places an order that stock rejects,
4. builds co-purchase data and fetches a recommendation,
5. cancels an order to show compensating events.

### Or run everything in Docker

```bash
docker compose --profile apps up -d --build
make demo
docker compose --profile apps down
```

> Run either `make dev` **or** the Docker `apps` profile, not both: they bind the
> same ports 8001–8005.

### Useful commands

```bash
make topics   # create Kafka topics (idempotent)
make logs     # tail Kafka + Postgres logs
make psql     # psql into orders_db
make test     # unit tests
make lint     # ruff checks
make down     # stop infra, keep Postgres data
make reset    # stop everything and delete volumes (fresh start)
```

## Repository map

```text
.
├── broker/                     # topic catalogue + create_topics.py
├── docs/STUDY_GUIDE.md         # read this to prepare for interviews
├── infra/postgres/init/        # creates one database per service
├── services/
│   ├── orders/
│   │   ├── app/
│   │   │   ├── handlers.py     # reactions to stock/invoice/fiscal events
│   │   │   ├── repository.py   # order writes + outbox event in one tx
│   │   │   └── main.py
│   │   └── migrations/
│   ├── stock/
│   ├── invoices/
│   ├── fiscal/
│   └── recommendations/
├── shared/edd_common/          # reusable event-driven plumbing
│   ├── events.py               # envelope + new_event()
│   ├── outbox.py               # transactional outbox + relay
│   ├── consumer.py             # manual offsets, inbox, retries, DLQ
│   └── runtime.py              # service lifecycle
├── scripts/demo.py             # end-to-end demonstration
├── docker-compose.yml
├── Makefile
└── pyproject.toml
```

## API examples

Order lifecycle (all asynchronous after the POST):

```bash
# Create
curl -s -X POST localhost:8001/orders \
  -H 'content-type: application/json' \
  -d '{
    "customer_id": "alice",
    "items": [
      {"product_id": "p-100", "quantity": 1, "unit_price_cents": 12900},
      {"product_id": "p-101", "quantity": 1, "unit_price_cents": 24900}
    ]
  }' | jq

# Poll the saga state
curl -s localhost:8001/orders/<order_id> | jq '.status, .stock_reserved_at, .invoice_issued_at, .fiscal_issued_at'

# Cancel (starts compensation)
curl -s -X POST localhost:8001/orders/<order_id>/cancel \
  -H 'content-type: application/json' \
  -d '{"reason":"changed my mind"}' | jq
```

Read models and operations:

```bash
curl -s localhost:8002/stock | jq
curl -s localhost:8002/reservations/<order_id> | jq
curl -s localhost:8003/invoices/by-order/<order_id> | jq
curl -s localhost:8004/fiscal-documents/by-order/<order_id> | jq
curl -s localhost:8004/fiscal-documents/<document_id>/xml
curl -s localhost:8005/recommendations/alice | jq
curl -s localhost:8005/stats | jq
```

OpenAPI docs: `http://localhost:8001/docs` … `http://localhost:8005/docs`.

## The event flow

```text
Client
  │ POST /orders
  ▼
Orders ── order.created ──────────────► Stock
  ▲                                       │ stock.reserved / stock.rejected
  │                                       ▼
  │                                   Invoices ── invoice.issued / invoice.voided ──► Fiscal
  │                                       ▲                                          │
  │                                       └────────────── orders.events ◄────────────┘
  │                                                             │
  └──────── stock.events + invoices.events + fiscal.events ◄───┘
        (when all three confirmations exist)
  │
  ├── order.completed ──► Stock (commit reservation)
  └── order.completed ──► Recommendations (update read model)
```

An order is `COMPLETED` only after stock reserved, invoice issued and fiscal document issued. A rejection or cancellation propagates compensating events (`stock.released`, `invoice.voided`, `fiscal.cancelled`).

## Design choices worth noticing

- **Transactional outbox**: the order row and `order.created` are written in the same Postgres transaction.
- **Idempotent inbox**: every consumer records `event_id` in `processed_events` in the same transaction as the business change.
- **Manual offsets**: Kafka offsets are committed only after the database transaction commits.
- **At-least-once + deduplication**: the pragmatic alternative to distributed exactly-once.
- **Partition key = `order_id`**: events for one order are ordered within a topic.
- **Saga choreography**: no central orchestrator; each service reacts and emits the next fact.
- **Compensation**: cancellation is an event, and each service undoes its own work.
- **CQRS read model**: recommendations is a projection built only from events.
- **Dead-letter topics**: poison messages go to `<service>.dlq` after retries.

## Troubleshooting

**Services fail to connect to Postgres**
Wait for `make up` to report Postgres healthy, then check `make logs`.

**Kafka topics are missing**
Run `make topics`. Consumers will wait, but producers need the topics in this setup.

**I want a clean slate**
`make reset` deletes the Postgres volume and Kafka data. Then run `make up` and `make dev`.

**Port already in use**
Stop the other process or change the port mapping / `http_port` setting.

**Why does the order take a few seconds to complete?**
Each hop is an outbox poll plus a consumer poll. The demo uses a 1s poll interval to make the asynchronous steps visible. Tune `outbox_poll_interval_seconds` and `consumer_poll_timeout_ms` for a snappier local run.
