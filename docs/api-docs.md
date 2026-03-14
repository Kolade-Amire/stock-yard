# Stock Insight Backend API Docs

Frontend integration reference for the backend currently implemented in this repository.

## Overview

- Base URL prefix: `/api/v1`
- Auth: none
- Content type: JSON for request and response bodies
- Chat is stateless: the frontend must send prior conversation turns on each `/chat` request
- Nullable fields: when data is unavailable, fields remain present and are returned as `null`
- Structured errors: non-2xx responses use the shared error envelope documented below

## Integration Notes

- Always send `sessionId` on `POST /api/v1/analytics/events`
  - The backend rate limiter works best when the frontend provides a consistent session identifier.
- `dataLimitations` is displayable metadata, not a hard error
  - Overview, news, and financial-summary endpoints may succeed with partial data and include limitations explaining what is missing.
- Chat conversation is frontend-supplied
  - The backend does not persist chat history.
  - The backend clips the provided conversation to the most recent configured turns before sending it to the model.
- Analytics rate limiting is in-process only
  - It is not shared across multiple app instances.
- Chat tool usage is model-dependent
  - `usedTools` may be empty on a valid response if the model answers from prior conversation context without calling tools.

## Shared Error Contract

All structured API errors use this shape:

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable explanation",
    "details": {}
  }
}
```

### Common error codes

- `VALIDATION_ERROR`
- `INVALID_SYMBOL`
- `INVALID_PERIOD_INTERVAL`
- `NOT_FOUND`
- `DATA_UNAVAILABLE`
- `PROVIDER_ERROR`
- `RATE_LIMITED`
- `LLM_ERROR`
- `INTERNAL_ERROR`

### Validation behavior

- FastAPI request-shape validation returns `422` with `code="VALIDATION_ERROR"`.
- Some semantic validation is handled in services and returns `400` with `code="VALIDATION_ERROR"` or a more specific error code.

## Endpoint Reference

### `GET /api/v1/health`

Simple liveness check.

**Request**

- No parameters

**Response**

```json
{
  "status": "ok"
}
```

**Typical status codes**

- `200 OK`
- `500 INTERNAL_ERROR` on unexpected server failure

**Example**

```bash
curl http://127.0.0.1:8000/api/v1/health
```

### `GET /api/v1/tickers/search`

Searches tickers by free-text query and returns normalized equity/ETF matches only.

**Query parameters**

- `q` required, string, minimum length `1`

**Behavior**

- Search results are filtered to quote types `EQUITY` and `ETF`.
- Results are normalized into a lightweight search shape.
- A whitespace-only query can still fail at the service layer with `400 VALIDATION_ERROR` after trimming.

**Response shape**

```json
{
  "query": "apple",
  "results": [
    {
      "symbol": "AAPL",
      "name": "Apple Inc.",
      "exchange": "NMS",
      "quoteType": "EQUITY"
    }
  ]
}
```

**Typical status codes**

- `200 OK`
- `400 VALIDATION_ERROR`
- `422 VALIDATION_ERROR`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/search?q=apple"
```

### `GET /api/v1/tickers/{symbol}`

Returns a normalized ticker overview.

**Path parameters**

- `symbol` required, Yahoo-style ticker symbol

**Behavior**

- Response shape is stable even when some fields are unavailable.
- Missing fields are `null`.
- `dataLimitations` explains important missing data, for example unavailable earnings date.

**Response shape**

```json
{
  "symbol": "AAPL",
  "overview": {
    "display_name": "Apple Inc.",
    "quote_type": "EQUITY",
    "exchange": "NMS",
    "currency": "USD",
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "website": "https://www.apple.com",
    "summary": "Company summary...",
    "current_price": 257.46,
    "previous_close": 260.03,
    "open_price": 258.63,
    "day_low": 254.37,
    "day_high": 258.77,
    "fifty_two_week_low": 169.21,
    "fifty_two_week_high": 288.62,
    "volume": 41094000,
    "average_volume": 43370120,
    "market_cap": 3784127902367.37,
    "trailing_pe": 32.58,
    "forward_pe": 27.71,
    "dividend_yield": 0.4,
    "beta": 1.116,
    "shares_outstanding": 14697926000,
    "analyst_target_mean": 292.15,
    "earnings_date": null,
    "is_etf": false
  },
  "dataLimitations": [
    "Earnings date is unavailable from the data provider."
  ]
}
```

**Typical status codes**

- `200 OK`
- `400 INVALID_SYMBOL`
- `404 NOT_FOUND`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL"
```

### `GET /api/v1/tickers/{symbol}/history`

Returns normalized OHLCV chart history for a curated set of periods and intervals.

**Path parameters**

- `symbol` required

**Query parameters**

- `period` required
  - allowed: `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `5y`, `max`
- `interval` required
  - allowed: `1m`, `5m`, `15m`, `1h`, `1d`, `1wk`, `1mo`

**Behavior**

- Unsupported period/interval combinations return `400 INVALID_PERIOD_INTERVAL`.
- Intraday combinations are restricted:
  - `1m`: `1d`, `5d`
  - `5m`, `15m`, `1h`: `1d`, `5d`, `1mo`
  - `1d`, `1wk`, `1mo`: all curated periods
- Bars are sorted ascending by timestamp.
- Rows missing OHLC values are dropped.
- Timestamps are returned as ISO-8601 UTC strings ending in `Z`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "period": "6mo",
  "interval": "1d",
  "bars": [
    {
      "timestamp": "2026-01-02T00:00:00Z",
      "open": 250.0,
      "high": 252.0,
      "low": 248.5,
      "close": 251.25,
      "adj_close": 251.25,
      "volume": 41234567
    }
  ]
}
```

**Typical status codes**

- `200 OK`
- `400 INVALID_SYMBOL`
- `400 INVALID_PERIOD_INTERVAL`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/history?period=6mo&interval=1d"
```

### `GET /api/v1/tickers/{symbol}/news`

Returns normalized ticker news items.

**Path parameters**

- `symbol` required

**Query parameters**

- `limit` optional, integer, default `10`, minimum `1`, maximum `50`

**Behavior**

- Response succeeds with an empty `news` array when the ticker is valid but no items are returned.
- `dataLimitations` explains missing or weak provider coverage.
- News items are normalized but may have nullable `publisher`, `link`, `published_at`, `summary`, or `source_type`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "news": [
    {
      "title": "Apple launches new feature",
      "publisher": "Example Publisher",
      "link": "https://example.com/story",
      "published_at": "2026-03-12T10:30:00Z",
      "summary": "Short summary...",
      "source_type": "STORY"
    }
  ],
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `400 INVALID_SYMBOL`
- `404 DATA_UNAVAILABLE`
- `422 VALIDATION_ERROR`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/news?limit=10"
```

### `GET /api/v1/tickers/{symbol}/financial-summary`

Returns normalized financial summary metrics.

**Path parameters**

- `symbol` required

**Behavior**

- Response shape is stable; unavailable values are `null`.
- `dataLimitations` explains missing key aggregates such as revenue, net income, or cash flow.

**Response shape**

```json
{
  "symbol": "AAPL",
  "financialSummary": {
    "revenue_ttm": 391035000000,
    "net_income_ttm": 117000000000,
    "ebitda": 134661000000,
    "gross_margins": 0.46,
    "operating_margins": 0.31,
    "profit_margins": 0.27,
    "free_cash_flow": 99584000000,
    "total_cash": 66952000000,
    "total_debt": 96961000000,
    "debt_to_equity": 151.5,
    "return_on_equity": 1.52,
    "return_on_assets": 0.22
  },
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `400 INVALID_SYMBOL`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/financial-summary"
```

### `POST /api/v1/analytics/events`

Records a frontend analytics event.

**Request body**

```json
{
  "symbol": "AAPL",
  "eventType": "view",
  "sessionId": "browser-session-id"
}
```

**Body fields**

- `symbol` required, ticker symbol
- `eventType` required
  - allowed: `search`, `view`, `chat_opened`, `chat_message`
- `sessionId` optional but strongly recommended

**Behavior**

- Events are stored in SQLite.
- The ingest rate limit is `60 events / 60 seconds / key`.
- Rate-limit key:
  - `client_ip + sessionId` when `sessionId` exists
  - otherwise `client_ip`
  - otherwise a shared fallback identity when no client IP is available
- Frontend should consistently send `sessionId`.
- Proxy headers are not trusted by default unless backend deployment enables that explicitly.

**Response shape**

```json
{
  "accepted": true,
  "symbol": "AAPL",
  "eventType": "view",
  "sessionId": "browser-session-id",
  "recordedAt": "2026-03-13T13:30:00Z"
}
```

**Typical status codes**

- `201 Created`
- `400 INVALID_SYMBOL`
- `400 VALIDATION_ERROR`
- `422 VALIDATION_ERROR`
- `429 RATE_LIMITED`
- `500 INTERNAL_ERROR`

**Representative rate-limit error**

```json
{
  "error": {
    "code": "RATE_LIMITED",
    "message": "Too many analytics events. Please retry shortly.",
    "details": {
      "limit": 60,
      "windowSeconds": 60,
      "retryAfterSeconds": 42
    }
  }
}
```

**Example**

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/analytics/events" \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","eventType":"view","sessionId":"browser-session-id"}'
```

### `GET /api/v1/analytics/popular`

Returns popularity aggregates for symbols over a time window.

**Query parameters**

- `window` optional, default `24h`
  - format: integer + unit
  - supported units: `h`, `d`
  - examples: `24h`, `7d`
  - maximum supported window: `30d`
- `limit` optional, integer, default `10`, minimum `1`, maximum `50`

**Behavior**

- Popularity score weights:
  - `search = 1`
  - `view = 2`
  - `chat_opened = 0`
  - `chat_message = 3`
- Response includes both score and raw event counts.
- Aggregation is over raw events within the window; there is no recency decay.

**Response shape**

```json
{
  "window": "24h",
  "limit": 10,
  "generatedAt": "2026-03-13T13:35:00Z",
  "results": [
    {
      "symbol": "AAPL",
      "score": 17,
      "totalEvents": 9,
      "searchEvents": 3,
      "viewEvents": 4,
      "chatOpenedEvents": 1,
      "chatMessageEvents": 1
    }
  ]
}
```

**Typical status codes**

- `200 OK`
- `400 VALIDATION_ERROR`
- `422 VALIDATION_ERROR`
- `500 INTERNAL_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/analytics/popular?window=24h&limit=10"
```

### `POST /api/v1/chat`

Ticker-scoped grounded chat endpoint.

**Request body**

```json
{
  "symbol": "AAPL",
  "message": "Given that outlook, what are the top near-term risks?",
  "conversation": [
    {
      "role": "user",
      "content": "summarize near-term outlook."
    },
    {
      "role": "assistant",
      "content": "AAPL's near-term outlook is generally positive..."
    }
  ]
}
```

**Body fields**

- `symbol` required
- `message` required, non-empty string
- `conversation` optional array of prior turns
  - each turn:
    - `role`: `user` or `assistant`
    - `content`: non-empty string

**Behavior**

- Backend is stateless and uses frontend-supplied conversation only.
- Prior conversation is clipped server-side before model invocation.
- Tool grounding is restricted to the active ticker symbol.
- `usedTools` may be empty on a successful response.
- Tool failures are usually converted into `limitations` instead of failing the whole request.
- Internal chat tools include:
  - `get_stock_snapshot`
  - `get_price_history`
  - `get_news_context`
  - `get_financial_summary`
  - `get_earnings_context`
  - `get_analyst_context`
- `get_earnings_context` and `get_analyst_context` are internal chat tools only, not standalone public HTTP endpoints.

**Response shape**

```json
{
  "symbol": "AAPL",
  "answer": "AAPL still looks constructive near term, but the main risks are ...",
  "highlights": [
    "Upcoming earnings are a key catalyst.",
    "Recent news flow adds uncertainty.",
    "Margins remain a core metric to watch."
  ],
  "usedTools": [
    "get_stock_snapshot",
    "get_earnings_context",
    "get_news_context"
  ],
  "limitations": [
    "Upcoming earnings date is unavailable from the data provider."
  ]
}
```

**Typical status codes**

- `200 OK`
- `400 INVALID_SYMBOL`
- `400 VALIDATION_ERROR`
- `422 VALIDATION_ERROR`
- `502 LLM_ERROR`

**Example**

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol":"AAPL",
    "message":"Given that thesis, has anything in the latest headlines or the upcoming earnings setup made the near-term case more fragile?",
    "conversation":[
      {"role":"user","content":"summarize near-term outlook."},
      {"role":"assistant","content":"AAPL'\''s near-term outlook is generally positive based on analyst sentiment and fundamental strength."}
    ]
  }'
```

### `GET /api/v1/market/movers`

Returns a normalized US market movers list for a supported screen.

**Query parameters**

- `screen` required
  - allowed: `gainers`, `losers`, `most_active`
- `limit` optional, integer, default `10`, minimum `1`, maximum `25`

**Behavior**

- Market scope is currently fixed to US and returned as `marketScope: "us"`.
- Internal Yahoo screener mapping:
  - `gainers -> day_gainers`
  - `losers -> day_losers`
  - `most_active -> most_actives`
- Rows missing all meaningful numeric market fields are dropped.
- If no usable mover rows remain, the endpoint returns `404 DATA_UNAVAILABLE`.
- Short cache TTL is used; data is intended for frequent refresh.

**Response shape**

```json
{
  "screen": "gainers",
  "marketScope": "us",
  "asOf": "2026-03-14T10:15:00Z",
  "results": [
    {
      "symbol": "AAPL",
      "name": "Apple Inc.",
      "exchange": "NMS",
      "quoteType": "EQUITY",
      "currentPrice": 257.46,
      "change": 3.12,
      "percentChange": 1.23,
      "volume": 41094000,
      "marketCap": 3784127902367.37
    }
  ],
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `400 VALIDATION_ERROR`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/market/movers?screen=gainers&limit=10"
```

### `GET /api/v1/market/benchmarks`

Returns a fixed curated list of benchmark ETFs/index funds with compact quote and fund metadata.

**Query parameters**

- none

**Behavior**

- The benchmark list is product-curated and currently includes:
  - `SPY` — S&P 500
  - `QQQ` — Nasdaq-100
  - `DIA` — Dow Jones Industrial Average
  - `IWM` — Russell 2000
  - `VTI` — Total US Stock Market
  - `BND` — US Aggregate Bond
- Endpoint prefers partial success:
  - if some funds fail, successful funds are still returned
  - top-level `dataLimitations` explains omissions
- `topHoldings` and `sectorWeights` are compact frontend-facing lists, not raw provider tables.
- The endpoint fails with `404 DATA_UNAVAILABLE` only if all curated benchmark items are unusable.

**Response shape**

```json
{
  "asOf": "2026-03-14T10:20:00Z",
  "funds": [
    {
      "symbol": "SPY",
      "benchmarkKey": "sp500",
      "benchmarkName": "S&P 500",
      "category": "large_cap_us",
      "displayName": "SPDR S&P 500 ETF Trust",
      "currentPrice": 585.1,
      "previousClose": 581.8,
      "dayChange": 3.3,
      "dayChangePercent": 0.57,
      "currency": "USD",
      "expenseRatio": 0.0009,
      "netAssets": 500000000000,
      "yield": 0.012,
      "fundFamily": "State Street",
      "topHoldings": [
        {
          "symbol": "AAPL",
          "name": "Apple",
          "holdingPercent": 0.07
        }
      ],
      "sectorWeights": [
        {
          "sector": "Technology",
          "weight": 0.35
        }
      ],
      "dataLimitations": []
    }
  ],
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/market/benchmarks"
```

### `GET /api/v1/market/earnings-calendar`

Returns a US earnings calendar window with normalized event rows.

**Query parameters**

- `start` optional, `YYYY-MM-DD`
  - default: today
- `end` optional, `YYYY-MM-DD`
  - default: `start + 7 days`
- `limit` optional, integer, default `25`, minimum `1`, maximum `100`
- `activeOnly` optional, boolean, default `true`

**Behavior**

- Valid empty ranges return `200` with `events: []`.
- Invalid date format or `end < start` returns `400 VALIDATION_ERROR`.
- `activeOnly=true` uses Yahoo's active-stock filter when building the calendar.
- `earningsDate` is returned as an ISO timestamp string because provider events are timed.
- Rows without a usable symbol or earnings date are dropped.

**Response shape**

```json
{
  "start": "2026-03-16",
  "end": "2026-03-23",
  "limit": 25,
  "activeOnly": true,
  "events": [
    {
      "symbol": "AAPL",
      "companyName": "Apple Inc.",
      "earningsDate": "2026-03-20T21:00:00Z",
      "reportTime": "After Market Close",
      "epsEstimate": 1.96,
      "reportedEps": null,
      "surprisePercent": null,
      "marketCap": 3100000000000
    }
  ],
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `400 VALIDATION_ERROR`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Examples**

```bash
curl "http://127.0.0.1:8000/api/v1/market/earnings-calendar"
```

```bash
curl "http://127.0.0.1:8000/api/v1/market/earnings-calendar?start=2026-03-16&end=2026-03-23&limit=50&activeOnly=true"
```

### `GET /api/v1/market/sectors/pulse`

Returns a market-wide sector summary across a fixed curated US sector list.

**Query parameters**

- none

**Behavior**

- Curated sector keys:
  - `basic-materials`
  - `communication-services`
  - `consumer-cyclical`
  - `consumer-defensive`
  - `energy`
  - `financial-services`
  - `healthcare`
  - `industrials`
  - `real-estate`
  - `technology`
  - `utilities`
- Pulse prefers partial success:
  - if some sectors fail, successful sectors are still returned
  - top-level `dataLimitations` records omitted sectors
- Each sector item contains trimmed summary lists:
  - top ETFs limited to `3`
  - top mutual funds limited to `3`
  - top companies limited to `3`

**Response shape**

```json
{
  "asOf": "2026-03-14T10:30:00Z",
  "sectors": [
    {
      "key": "technology",
      "name": "Technology",
      "symbol": "TEC",
      "overview": {
        "companiesCount": 120,
        "marketCap": 1234567890,
        "messageBoardId": null,
        "description": "Technology overview...",
        "industriesCount": 8,
        "marketWeight": 0.15,
        "employeeCount": 500000
      },
      "topEtfs": [
        {
          "symbol": "XLK",
          "name": "Technology Select Sector SPDR"
        }
      ],
      "topMutualFunds": [
        {
          "symbol": "VITAX",
          "name": "Vanguard Information Technology Index Fund"
        }
      ],
      "topCompanies": [
        {
          "symbol": "AAPL",
          "name": "Apple",
          "rating": "A",
          "marketWeight": 0.18
        }
      ],
      "dataLimitations": []
    }
  ],
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/market/sectors/pulse"
```

### `GET /api/v1/market/sectors/{sector_key}`

Returns full detail for a single curated sector key.

**Path parameters**

- `sector_key` required
  - allowed values:
    - `basic-materials`
    - `communication-services`
    - `consumer-cyclical`
    - `consumer-defensive`
    - `energy`
    - `financial-services`
    - `healthcare`
    - `industrials`
    - `real-estate`
    - `technology`
    - `utilities`

**Behavior**

- Invalid sector keys return `400 VALIDATION_ERROR` with an allowlist in `details.allowedSectorKeys`.
- Detail endpoint is not partial-success:
  - if the requested sector cannot be resolved, the request fails
- `topEtfs`, `topMutualFunds`, `topCompanies`, and `industries` are full normalized lists for that sector.

**Response shape**

```json
{
  "key": "technology",
  "name": "Technology",
  "symbol": "TEC",
  "overview": {
    "companiesCount": 120,
    "marketCap": 1234567890,
    "messageBoardId": null,
    "description": "Technology overview...",
    "industriesCount": 8,
    "marketWeight": 0.15,
    "employeeCount": 500000
  },
  "topEtfs": [
    {
      "symbol": "XLK",
      "name": "Technology Select Sector SPDR"
    }
  ],
  "topMutualFunds": [
    {
      "symbol": "VITAX",
      "name": "Vanguard Information Technology Index Fund"
    }
  ],
  "topCompanies": [
    {
      "symbol": "AAPL",
      "name": "Apple",
      "rating": "A",
      "marketWeight": 0.18
    }
  ],
  "industries": [
    {
      "key": "software",
      "name": "Software",
      "symbol": "^SWS",
      "marketWeight": 0.35
    }
  ],
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `400 VALIDATION_ERROR`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/market/sectors/technology"
```

## Field Notes for Frontend Rendering

- `dataLimitations`
  - Present on overview, news, financial summary, and market discovery endpoints.
  - Safe to render as informational warnings or subtle badges.
- Numeric fields
  - Many values come directly from provider normalization and may be `null`.
  - Frontend should not assume any metric exists for all symbols.
- History bars
  - Use `bars` from the history endpoint for charting.
  - The overview endpoint is not intended to drive charts.
- Chat `usedTools`
  - Useful for debugging, observability, or optional developer UI.
  - Do not treat an empty array as a failure.
- Market endpoints
  - `market/movers` is short-lived discovery data and suitable for frequent refresh.
  - `market/benchmarks`, `market/earnings-calendar`, and `market/sectors/*` are read-oriented discovery surfaces and may return partial data with stable response shapes.
