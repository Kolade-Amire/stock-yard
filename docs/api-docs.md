# Stock Insight Backend API Docs

Frontend integration reference for the backend currently implemented in this repository.

## Overview

- Base URL prefix: `/api/v1`
- Auth: none
- Content type: JSON for request and response bodies
- Chat uses frontend-supplied conversation plus server-managed session memoization keyed by `sessionId`
- Nullable fields: when data is unavailable, fields remain present and are returned as `null`
- Structured errors: non-2xx responses use the shared error envelope documented below

## Integration Notes

- Always send `sessionId` on `POST /api/v1/analytics/events`
  - The backend rate limiter works best when the frontend provides a consistent session identifier.
- `dataLimitations` is displayable metadata, not a hard error
  - Overview, news, and financial-summary endpoints may succeed with partial data and include limitations explaining what is missing.
- Some deeper data endpoints are equity-heavy
  - ETF symbols often have sparse or empty statement, earnings, analyst, and ownership tables from Yahoo.
  - For those cases the backend returns either nullable fields plus `dataLimitations`, or `404 DATA_UNAVAILABLE` if there is no material usable dataset.
- Chat conversation is frontend-supplied
  - The backend does not persist full chat transcripts.
  - The backend clips the provided conversation to the most recent configured turns before sending it to the model.
  - The backend does memoize compact prior tool results server-side per `sessionId`.
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
  - allowed: `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, `max`
- `interval` required
  - allowed: `1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`, `5d`, `1wk`, `1mo`, `3mo`

**Behavior**

- Unsupported period/interval combinations return `400 INVALID_PERIOD_INTERVAL`.
- Intraday combinations are restricted:
  - `1m`: `1d`, `5d`
  - `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`: `1d`, `5d`, `1mo`
  - `1d`, `5d`, `1wk`, `1mo`, `3mo`: all curated periods
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

### `GET /api/v1/tickers/{symbol}/financials/trends`

Returns chart-friendly annual and quarterly financial trend series.

**Path parameters**

- `symbol` required

**Behavior**

- Built from normalized income-statement and cash-flow rows.
- `annual` and `quarterly` are sorted ascending by `periodEnd`.
- `freeCashFlow` may be provider-supplied or derived from `operatingCashFlow + capitalExpenditure` when Yahoo omits the direct row.
- ETF symbols often have no material statement tables and may return `404 DATA_UNAVAILABLE`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "annual": [
    {
      "periodEnd": "2024-09-30",
      "revenue": 391035000000,
      "netIncome": 93736000000,
      "operatingCashFlow": 118254000000,
      "capitalExpenditure": -9447000000,
      "freeCashFlow": 108807000000
    }
  ],
  "quarterly": [
    {
      "periodEnd": "2025-12-31",
      "revenue": 143756000000,
      "netIncome": 42097000000,
      "operatingCashFlow": 53925000000,
      "capitalExpenditure": -2373000000,
      "freeCashFlow": 51552000000
    }
  ],
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
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/financials/trends"
```

### `GET /api/v1/tickers/{symbol}/earnings/history`

Returns normalized earnings surprise history.

**Path parameters**

- `symbol` required

**Behavior**

- Rows are built from Yahoo earnings-history tables and sorted ascending by `reportDate`.
- `quarter` is derived from the report-period end date.
- Symbols with no material earnings history return `404 DATA_UNAVAILABLE`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "events": [
    {
      "reportDate": "2025-12-31",
      "quarter": "Q4 2025",
      "epsEstimate": 2.6708,
      "epsActual": 2.84,
      "surprisePercent": 0.0634
    }
  ],
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
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/earnings/history"
```

### `GET /api/v1/tickers/{symbol}/earnings/estimates`

Returns normalized EPS, revenue, and growth estimate tables.

**Path parameters**

- `symbol` required

**Behavior**

- Endpoint can succeed with partial sections.
- `epsEstimates`, `revenueEstimates`, and `growthEstimates` are separate arrays because Yahoo coverage may differ by table.
- If all three sections are empty, the endpoint returns `404 DATA_UNAVAILABLE`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "epsEstimates": [
    {
      "period": "0q",
      "avg": 1.9542,
      "low": 1.85,
      "high": 2.16,
      "yearAgoEps": 1.65,
      "numberOfAnalysts": 29,
      "growth": 0.1844
    }
  ],
  "revenueEstimates": [
    {
      "period": "0q",
      "avg": 109068626370,
      "low": 105000000000,
      "high": 112596000000,
      "numberOfAnalysts": 30,
      "yearAgoRevenue": 95359000000,
      "growth": 0.1438
    }
  ],
  "growthEstimates": [
    {
      "period": "0q",
      "stockTrend": 0.1855,
      "indexTrend": 0.133
    }
  ],
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
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/earnings/estimates"
```

### `GET /api/v1/tickers/compare`

Returns normalized multi-symbol comparison series.

**Query parameters**

- `symbols` required, comma-separated ticker list
  - minimum `2` unique symbols
  - maximum `5` unique symbols
- `period` required
  - allowed: `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `5y`, `max`
- `interval` required
  - allowed: `1m`, `5m`, `15m`, `1h`, `1d`, `1wk`, `1mo`

**Behavior**

- Uses the same period/interval validation rules as `/tickers/{symbol}/history`.
- Comparison is all-or-nothing on requested symbol history:
  - if one requested symbol cannot produce usable history, the request fails
- Benchmark ETF comparison is supported by passing symbols like `SPY`, `QQQ`, `DIA`, `IWM`, `VTI`, or `BND`.
- `bars` reuse the same shape as the single-symbol history endpoint.

**Response shape**

```json
{
  "symbols": ["AAPL", "MSFT", "SPY"],
  "period": "6mo",
  "interval": "1d",
  "series": [
    {
      "symbol": "AAPL",
      "displayName": "Apple Inc.",
      "currentPrice": 250.12,
      "changePercent": -2.18,
      "bars": [
        {
          "timestamp": "2026-03-13T04:00:00Z",
          "open": 255.4,
          "high": 256.33,
          "low": 249.52,
          "close": 250.12,
          "adj_close": 250.12,
          "volume": 34193754
        }
      ]
    }
  ],
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `400 VALIDATION_ERROR`
- `400 INVALID_SYMBOL`
- `400 INVALID_PERIOD_INTERVAL`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/compare?symbols=AAPL,MSFT,SPY&period=6mo&interval=1d"
```

### `GET /api/v1/tickers/{symbol}/analyst/summary`

Returns a current analyst snapshot suitable for headline panels.

**Path parameters**

- `symbol` required

**Behavior**

- Summary includes current target bands, the latest recommendation breakdown, and the count of recent analyst actions within the configured backend window.
- `recentActionCount` is currently measured over the last `90` days.
- Endpoint may succeed with partial target/recommendation coverage and report gaps via `dataLimitations`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "analystSummary": {
    "currentPriceTarget": 250.12,
    "targetLow": 205.0,
    "targetHigh": 350.0,
    "targetMean": 295.43536,
    "targetMedian": 300.0,
    "recommendationSummary": {
      "period": "0m",
      "strongBuy": 6,
      "buy": 25,
      "hold": 15,
      "sell": 1,
      "strongSell": 1
    },
    "recentActionCount": 21,
    "recentActionWindowDays": 90
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
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/analyst/summary"
```

### `GET /api/v1/tickers/{symbol}/analyst/history`

Returns recommendation-history rows and recent analyst action events.

**Path parameters**

- `symbol` required

**Behavior**

- `recommendationHistory` is normalized from Yahoo recommendation tables.
- `actions` is a recent-window analyst action timeline, sorted newest first.
- Action fields such as `firm`, `toGrade`, and `fromGrade` may be `null` because Yahoo often omits them even when price-target actions are present.
- If both sections are empty, the endpoint returns `404 DATA_UNAVAILABLE`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "recommendationHistory": [
    {
      "period": "0m",
      "strongBuy": 6,
      "buy": 25,
      "hold": 15,
      "sell": 1,
      "strongSell": 1
    }
  ],
  "actions": [
    {
      "gradedAt": "2026-03-05T13:56:15Z",
      "firm": null,
      "toGrade": null,
      "fromGrade": null,
      "action": null,
      "priceTargetAction": "Maintains",
      "currentPriceTarget": 350.0,
      "priorPriceTarget": 350.0
    }
  ],
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
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/analyst/history"
```

### `GET /api/v1/tickers/{symbol}/ownership`

Returns ownership and holder tables.

**Path parameters**

- `symbol` required

**Query parameters**

- `section` optional, default `all`
  - allowed:
    - `all`
    - `institutional`
    - `mutual_funds`
    - `insider_roster`
- `limit` optional, integer, default `25`, minimum `1`, maximum `100`
- `offset` optional, integer, default `0`, minimum `0`

**Behavior**

- `majorHolders` is a normalized metric list, not a raw provider table.
- `majorHolders` is never paged.
- `institutionalHolders`, `mutualFundHolders`, and `insiderRoster` are paged independently using the same requested `limit` and `offset`.
- `section=all` returns all three paged holder arrays plus pagination metadata for all three sections.
- Section-specific requests return only the requested holder array; non-requested arrays are `[]` and non-requested pagination objects are `null`.
- ETF and thinly covered symbols may have no material ownership tables and return `404 DATA_UNAVAILABLE`.
- Negative offsets return `400 VALIDATION_ERROR`.
- Unsupported `section` values return `400 VALIDATION_ERROR`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "requestedSection": "all",
  "limit": 5,
  "offset": 0,
  "majorHolders": [
    {
      "key": "insidersPercentHeld",
      "label": "Insiders Percent Held",
      "value": 0.01637
    }
  ],
  "institutionalHolders": [
    {
      "dateReported": "2025-12-31T00:00:00Z",
      "holder": "Vanguard Group Inc",
      "pctHeld": 0.097200006,
      "shares": 1426283914,
      "value": 356742125605,
      "pctChange": 0.019199999
    }
  ],
  "mutualFundHolders": [],
  "insiderRoster": [],
  "institutionalPagination": {
    "offset": 0,
    "limit": 5,
    "returnedCount": 5,
    "totalAvailable": 10,
    "hasMore": true,
    "nextOffset": 5
  },
  "mutualFundPagination": {
    "offset": 0,
    "limit": 5,
    "returnedCount": 5,
    "totalAvailable": 10,
    "hasMore": true,
    "nextOffset": 5
  },
  "insiderRosterPagination": {
    "offset": 0,
    "limit": 5,
    "returnedCount": 5,
    "totalAvailable": 10,
    "hasMore": true,
    "nextOffset": 5
  },
  "dataLimitations": [
    "Mutual fund holders are unavailable from the data provider.",
    "Insider roster is unavailable from the data provider."
  ]
}
```

**Typical status codes**

- `200 OK`
- `400 INVALID_SYMBOL`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/ownership?section=all&limit=5&offset=0"
```

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/ownership?section=institutional&limit=5&offset=5"
```

### `GET /api/v1/tickers/{symbol}/options/expirations`

Returns available option expiration dates for an optionable symbol.

**Path parameters**

- `symbol` required

**Behavior**

- Response is a flat list of ISO dates.
- Non-optionable symbols return `404 DATA_UNAVAILABLE`.

**Response shape**

```json
{
  "symbol": "AAPL",
  "expirations": ["2026-03-16", "2026-03-18", "2026-03-20"]
}
```

**Typical status codes**

- `200 OK`
- `400 INVALID_SYMBOL`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/options/expirations"
```

### `GET /api/v1/tickers/{symbol}/options/chain`

Returns a normalized call/put chain for a selected expiration.

**Path parameters**

- `symbol` required

**Query parameters**

- `expiration` required, `YYYY-MM-DD`

**Behavior**

- Invalid date format returns `400 VALIDATION_ERROR`.
- Unsupported expirations for a valid symbol return `400 VALIDATION_ERROR` with `details.allowedExpirations`.
- `underlyingPrice` is resolved from quote data and may be `null`.
- Call and put arrays may be large for liquid symbols.

**Response shape**

```json
{
  "symbol": "AAPL",
  "expiration": "2026-03-16",
  "underlyingPrice": 250.12,
  "calls": [
    {
      "contractSymbol": "AAPL260316C00250000",
      "lastTradeDate": "2026-03-13T19:59:57Z",
      "strike": 250.0,
      "lastPrice": 2.06,
      "bid": 2.11,
      "ask": 2.23,
      "change": -4.34,
      "percentChange": -67.8125,
      "volume": 6720,
      "openInterest": 166,
      "impliedVolatility": 0.23987576538085933,
      "inTheMoney": true,
      "contractSize": "REGULAR",
      "currency": "USD"
    }
  ],
  "puts": [],
  "dataLimitations": [
    "Put contracts are unavailable from the data provider."
  ]
}
```

**Typical status codes**

- `200 OK`
- `400 INVALID_SYMBOL`
- `400 VALIDATION_ERROR`
- `404 DATA_UNAVAILABLE`
- `502 PROVIDER_ERROR`

**Example**

```bash
curl "http://127.0.0.1:8000/api/v1/tickers/AAPL/options/chain?expiration=2026-03-16"
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
  "sessionId": "chat-session-id",
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
- `sessionId` optional
  - Omit it on the first turn.
  - Reuse the returned value on follow-up turns for the same symbol.
- `message` required, non-empty string
- `conversation` optional array of prior turns
  - each turn:
    - `role`: `user` or `assistant`
    - `content`: non-empty string

**Behavior**

- Backend uses frontend-supplied conversation plus server-managed memoized tool state keyed by `sessionId`.
- Prior conversation is clipped server-side before model invocation.
- Tool grounding is restricted to the active ticker symbol.
- If `sessionId` is omitted or unknown, the backend creates a new chat session and returns it.
- If the frontend reuses a `sessionId` against a different symbol, the backend starts a new session and returns a new `sessionId`.
- Cached session evidence may reduce repeated tool calls on follow-up turns, but `usedTools` still reports only tools called during the current request.
- `usedTools` may be empty on a successful response.
- Tool failures are usually converted into `limitations` instead of failing the whole request.
- Internal chat tools include:
  - `get_stock_snapshot`
  - `get_price_history`
  - `get_news_context`
  - `get_financial_summary`
  - `get_financial_trends_context`
  - `get_earnings_deep_context`
  - `get_analyst_deep_context`
  - `get_ownership_context`
- These are internal chat tools only, not standalone public HTTP endpoints.

**Response shape**

```json
{
  "symbol": "AAPL",
  "sessionId": "chat-session-id",
  "answer": "AAPL still looks constructive near term, but the main risks are ...",
  "highlights": [
    "Upcoming earnings are a key catalyst.",
    "Recent news flow adds uncertainty.",
    "Margins remain a core metric to watch."
  ],
  "usedTools": [
    "get_stock_snapshot",
    "get_earnings_deep_context",
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
    "sessionId":"chat-session-id",
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
- `offset` optional, integer, default `0`, minimum `0`
- `activeOnly` optional, boolean, default `true`

**Behavior**

- Valid empty ranges return `200` with `events: []`.
- Invalid date format or `end < start` returns `400 VALIDATION_ERROR`.
- Negative offsets return `400 VALIDATION_ERROR`.
- `activeOnly=true` uses Yahoo's active-stock filter when building the calendar.
- `earningsDate` is returned as an ISO timestamp string because provider events are timed.
- Rows without a usable symbol or earnings date are dropped.
- Pagination is offset-based.
- `returnedCount`, `hasMore`, and `nextOffset` drive frontend paging controls.
- Exact total available rows are not exposed for this endpoint.

**Response shape**

```json
{
  "start": "2026-03-16",
  "end": "2026-03-23",
  "limit": 25,
  "offset": 0,
  "activeOnly": true,
  "returnedCount": 25,
  "hasMore": true,
  "nextOffset": 25,
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
curl "http://127.0.0.1:8000/api/v1/market/earnings-calendar?start=2026-03-16&end=2026-03-23&limit=50&offset=50&activeOnly=true"
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

### `GET /api/v1/market/industries/{industry_key}`

Returns industry drill-down detail for an industry key discovered from a sector-detail response.

**Path parameters**

- `industry_key` required
  - keys come from `GET /api/v1/market/sectors/{sector_key}` response `industries[].key`

**Behavior**

- Invalid or unsupported keys return `400 VALIDATION_ERROR`.
- This endpoint is not based on a fixed allowlist; it is intended to follow the industry keys exposed by sector detail.
- Some company names or ratings may be `null` when Yahoo omits them.

**Response shape**

```json
{
  "key": "software-infrastructure",
  "name": "Software - Infrastructure",
  "symbol": "^YH31110030",
  "sectorKey": "technology",
  "sectorName": "Technology",
  "overview": {
    "companiesCount": 196,
    "marketCap": 4775130169344,
    "messageBoardId": "INDEXYH31110030",
    "description": "Companies that develop, design, support, and provide system software and services...",
    "marketWeight": 0.21729653,
    "employeeCount": 824379
  },
  "topCompanies": [
    {
      "symbol": "MSFT",
      "name": "Microsoft Corporation",
      "rating": "Strong Buy",
      "marketWeight": 0.6156912
    }
  ],
  "topGrowthCompanies": [
    {
      "symbol": "CFLT",
      "name": "Confluent, Inc.",
      "ytdReturn": 0.0142,
      "growthEstimate": 5.5
    }
  ],
  "topPerformingCompanies": [
    {
      "symbol": "TCGL",
      "name": "TechCreate Group Ltd.",
      "ytdReturn": 32.2385,
      "lastPrice": 172.84,
      "targetPrice": null
    }
  ],
  "dataLimitations": []
}
```

**Typical status codes**

- `200 OK`
- `400 VALIDATION_ERROR`
- `502 PROVIDER_ERROR`

**Examples**

```bash
curl "http://127.0.0.1:8000/api/v1/market/industries/software-infrastructure"
```

```bash
curl "http://127.0.0.1:8000/api/v1/market/industries/not-a-real-industry"
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
- Deeper ticker tables
  - `financials/trends`, `earnings/*`, `analyst/*`, `ownership`, and `options/*` are richer research surfaces.
  - Some of them are sparse or unavailable for ETFs even when quote/history endpoints work normally.
- Chat `usedTools`
  - Useful for debugging, observability, or optional developer UI.
  - Do not treat an empty array as a failure.
- Market endpoints
  - `market/movers` is short-lived discovery data and suitable for frequent refresh.
  - `market/benchmarks`, `market/earnings-calendar`, `market/sectors/*`, and `market/industries/*` are read-oriented discovery surfaces and may return partial data with stable response shapes.
