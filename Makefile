.PHONY: help up down logs topics dev demo test lint fmt docker-up reset psql

help:
	@echo "Event-Driven Shop"
	@echo ""
	@echo "  make up         Start Postgres + Kafka + Kafka UI (Docker)"
	@echo "  make dev        Start all five FastAPI services (Ctrl-C stops them)"
	@echo "  make demo       Run the end-to-end demo against localhost"
	@echo "  make topics     Create Kafka topics"
	@echo "  make down       Stop infrastructure (keeps Postgres data)"
	@echo "  make reset      Stop infrastructure and delete Postgres volume"
	@echo "  make logs       Tail Kafka/Postgres logs"
	@echo "  make test       Run unit tests"
	@echo "  make lint       ruff check + format check"
	@echo "  make docker-up  Run infrastructure AND services in Docker"
	@echo "  make psql       Open a psql shell"

up:
	docker compose up -d postgres kafka kafka-ui

down:
	docker compose down

logs:
	docker compose logs -f --tail=100 postgres kafka

topics:
	uv run python broker/create_topics.py

dev:
	./scripts/run_all.sh

demo:
	uv run python scripts/demo.py

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check . --fix
	uv run ruff format .

docker-up:
	docker compose --profile apps up -d --build

reset:
	docker compose --profile apps down -v

psql:
	docker compose exec postgres psql -U shop -d orders_db
