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

## Proxy header trust

Analytics ingest rate limiting uses the actual client socket by default:

```env
TRUST_PROXY_HEADERS=false
```

Set `TRUST_PROXY_HEADERS=true` only when the app is behind a trusted reverse proxy that strips
untrusted `X-Forwarded-For` headers. Leave it `false` for local development or direct app exposure.

## Quick check

```bash
curl http://127.0.0.1:8000/api/v1/health
```

- API integration reference: [docs/api-docs.md](docs/api-docs.md)