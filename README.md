# Pynchronize - Quick Start Guide

## Local Development

### Install Dependencies

```bash
uv sync --group dev
```

### Run Unit Tests

```bash
uv run pytest tests/unit/ -v
```

### Run Single Node

```bash
cp .env.example .env
uv run python -m src.main
```

API available at `http://localhost:8100`

## Docker Cluster (3 Nodes)

### Start Cluster

```bash
cd docker
docker-compose up -d
```

### Verify Health

```bash
curl http://localhost:8101/health
curl http://localhost:8102/health
curl http://localhost:8103/health
```

### Stop Cluster

```bash
docker-compose down
```

## API Examples

### Lock Manager

```bash
curl -X POST http://localhost:8101/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"client_id": "c1", "resource_id": "r1", "lock_type": "exclusive"}'

curl -X POST http://localhost:8101/lock/release \
  -H "Content-Type: application/json" \
  -d '{"client_id": "c1", "resource_id": "r1"}'
```

### Queue

```bash
curl -X POST http://localhost:8101/queue/publish \
  -H "Content-Type: application/json" \
  -d '{"queue_name": "test", "payload": {"msg": "hello"}}'

curl -X POST http://localhost:8101/queue/consume \
  -H "Content-Type: application/json" \
  -d '{"queue_name": "test", "limit": 1}'
```
