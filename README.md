# Stock Insight Backend

FastAPI backend for stock exploration and ticker-grounded AI chat.

## Run locally

```bash
uv sync
cp .env.example .env
uv run uvicorn app.main:app --reload
```

## Chat cost controls

These defaults favor a balanced quality/cost tradeoff for grounded chat:

```env
CHAT_MAX_TURNS=6
CHAT_MAX_TOOL_CALL_ROUNDS=2
CHAT_HISTORY_RECENT_BARS_LIMIT=12
CHAT_NEWS_TOOL_DEFAULT_LIMIT=3
CHAT_TOOL_GATING_MODE=balanced
```

## Quick check

```bash
curl http://127.0.0.1:8000/api/v1/health
```
