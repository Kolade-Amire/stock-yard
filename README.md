# Stock Insight Backend

FastAPI backend for stock exploration and ticker-grounded AI chat.

## Current status

Pass 1 is implemented:
- project scaffold and dependency manifest
- environment-based settings
- shared structured error handling
- `GET /api/v1/health`

## Run locally

```bash
uv sync
cp .env.example .env
uv run uvicorn app.main:app --reload
```

## Quick check

```bash
curl http://127.0.0.1:8000/api/v1/health
```
