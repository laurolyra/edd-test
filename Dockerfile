FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY shared ./shared
RUN uv sync --frozen --no-dev

COPY services ./services
COPY broker ./broker

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-c", "print('Pick a service command from docker-compose.yml')"]
