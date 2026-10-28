#!/usr/bin/env bash
# Start all five services locally. Ctrl-C stops the whole stack.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running. Start Docker Desktop, then run 'make up'." >&2
  exit 1
fi

if ! docker compose ps postgres --status running --quiet | grep -q .; then
  echo "Postgres is not running. Starting local infrastructure..."
  docker compose up -d postgres kafka kafka-ui
fi

echo "Installing Python dependencies..."
uv sync --quiet

echo "Creating Kafka topics..."
"$ROOT/.venv/bin/python" "$ROOT/broker/create_topics.py"

mkdir -p "$ROOT/.run"
pids=()

cleanup() {
  echo
  echo "Stopping services..."
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "Stopped. Logs are in .run/"
}
trap cleanup EXIT INT TERM

start_service() {
  local name="$1"
  local port="$2"
  echo "Starting $name on http://localhost:$port (log: .run/$name.log)"
  (
    cd "$ROOT/services/$name"
    exec "$ROOT/.venv/bin/uvicorn" app.main:app \
      --host 127.0.0.1 \
      --port "$port" \
      --log-level info
  ) >"$ROOT/.run/$name.log" 2>&1 &
  pids+=("$!")
}

start_service orders 8001
start_service stock 8002
start_service invoices 8003
start_service fiscal 8004
start_service recommendations 8005

echo
echo "All services launched. OpenAPI docs:"
echo "  orders          http://localhost:8001/docs"
echo "  stock           http://localhost:8002/docs"
echo "  invoices        http://localhost:8003/docs"
echo "  fiscal          http://localhost:8004/docs"
echo "  recommendations http://localhost:8005/docs"
echo
echo "Run 'make demo' in another terminal. Press Ctrl-C here to stop."
echo

# macOS ships bash 3.2, where `wait -n` does not exist. Poll instead and fail
# fast if any child process disappears.
while true; do
  for pid in "${pids[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "A service exited. Check .run/*.log" >&2
      exit 1
    fi
  done
  sleep 2
done
