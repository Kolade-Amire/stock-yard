## Stock Insight Backend V1 Spec

### 1) Product summary

We are building a **backend-first stock exploration and AI Q&A app**.

A user enters a ticker symbol such as `AAPL`, `MSFT`, or `SPY`. The backend fetches market and company data from **yfinance**, normalizes it, and exposes it through a clean FastAPI API for the frontend. On the same stock page, the user can open an **AI chat** and ask questions about the currently viewed ticker. The AI must answer using only the data the backend provides for that ticker, especially price history, key company/fund stats, financial summaries, and recent ticker-relevant news. yfinance supports ticker data such as `info`, `fast_info`, `history`, financial statements, analyst and earnings-related data, ticker news, and search/lookup results that can include quotes and news. ([ranaroussi.github.io][1])

This is a **weekend portfolio project**, so V1 is intentionally narrow:

* no auth
* no persistent chat history
* no favorites/watchlists
* no trade execution
* no broker integration
* no recommendation engine pretending to be financial advice

There will be a tiny persistence layer only for **anonymous popularity analytics** such as “most searched stocks” or “most viewed stocks.” SQLite is the right fit for that because it is embedded, file-based, serverless, and available directly from Python’s standard library via `sqlite3`. ([SQLite][2])

Also note: yfinance’s own docs state it is intended for research and educational purposes, and Yahoo Finance data is intended for personal use only. That is fine for this project, but this backend must be treated as a demo/prototype, not a commercial data backend. ([ranaroussi.github.io][3])

---

### 2) Core V1 goals

V1 must support four core flows:

1. **Ticker detail retrieval**
   User requests a ticker and receives normalized overview data suitable for cards, stat panels, and summary UI.

2. **Chart/history retrieval**
   User requests historical price data for a ticker with a chosen period and interval, and receives chart-ready OHLCV output.

3. **Ticker-grounded AI chat**
   User asks questions about the currently viewed ticker. The AI must use backend-provided tools and structured ticker context, not hallucinated market knowledge.

4. **Anonymous stock popularity tracking**
   The backend records ticker search/view activity and exposes endpoints for trending/popular stocks.

These flows can all be implemented cleanly with FastAPI using a multi-file project layout, dependency injection, and environment-based settings. FastAPI’s docs explicitly recommend larger apps be split across routers/modules, and recommend settings management with Pydantic-based configuration. ([fastapi.tiangolo.com][4])

---

### 3) V1 asset scope

V1 supports:

* **equities**
* **ETFs**

V1 does not explicitly support:

* options chains in UI/chat
* crypto-specific flows
* forex-specific flows
* multi-ticker compare
* portfolio analysis
* real-time streaming quotes

This is deliberate. yfinance supports much more, including screener/query functionality and WebSocket streaming, but V1 should stay focused on single-ticker exploration and grounded Q&A. ([ranaroussi.github.io][5])

---

### 4) LLM policy

The backend must be built around an **LLM provider abstraction**.

Primary provider:

* **Gemini API**

Optional development providers:

* **LM Studio local server**
* any **OpenAI-compatible local model endpoint** exposed by LM Studio

Gemini is the official default because the project is explicitly meant to use Gemini. Gemini supports **function calling** for tool use and **structured outputs** using JSON Schema, which is exactly what this stock chat feature needs. LM Studio supports **OpenAI-compatible endpoints**, so the same high-level provider contract can be swapped to a local server by changing configuration. ([Google AI for Developers][6])

The application must not hardcode Gemini-specific logic into business services. It must isolate model-specific code behind provider adapters.

---

### 5) AI behavior rules

The AI is not a generic chatbot. It is a **ticker-scoped analysis assistant**.

It must:

* answer only about the currently active ticker
* use backend tools to retrieve data
* distinguish facts from interpretation
* admit when data is unavailable
* avoid fabricating metrics or events
* avoid personalized financial advice
* avoid absolute buy/sell directives

It may:

* summarize price action
* explain major stats
* summarize recent news themes
* relate fundamentals to recent stock movement
* explain what metrics usually mean
* discuss analyst targets and earnings calendar if available

It must not:

* claim access to data the backend did not provide
* answer questions as if it has live institutional market feeds
* provide certainty where data is incomplete
* answer off-topic market questions unrelated to the active ticker unless the product explicitly adds that later

Gemini tool use should be implemented with function calling, and the final response shape should be constrained with structured output so the frontend gets predictable fields. Streaming can be added as a chat enhancement using Gemini’s streaming content generation endpoints over SSE, but this is optional in the first cut. ([Google AI for Developers][6])

---

### 6) Backend architecture

The backend architecture must separate:

* HTTP transport
* market data retrieval
* normalization/mapping
* analytics persistence
* AI provider calls
* chat orchestration

Recommended structure:

```text
app/
  main.py
  api/
    routers/
      health.py
      tickers.py
      chat.py
      analytics.py
  core/
    config.py
    logging.py
    errors.py
    dependencies.py
  domain/
    models.py
    enums.py
  schemas/
    common.py
    ticker.py
    history.py
    news.py
    chat.py
    analytics.py
  services/
    yfinance_service.py
    ticker_context_service.py
    analytics_service.py
    chat_service.py
  providers/
    llm/
      base.py
      gemini_provider.py
      openai_compat_provider.py
  repositories/
    analytics_repo.py
  db/
    sqlite.py
  utils/
    time.py
    mappers.py
    symbols.py
tests/
  ...
```

This structure follows FastAPI’s recommended approach for bigger applications: modular routers, shared dependencies, and isolated service/config layers. ([fastapi.tiangolo.com][4])

---

### 7) Data sources and what to fetch from yfinance

For a single ticker page, the backend should be able to fetch from yfinance:

* `info`
* `fast_info`
* `history`
* `get_news(...)`
* selected financial statement / earnings / analyst methods when useful
* search/lookup data for symbol discovery and validation

Relevant yfinance coverage for this plan includes:

* `Ticker.info`
* `Ticker.fast_info`
* `Ticker.get_news(...)`
* `Ticker.history(...)`
* analyst price targets / recommendations / earnings-related methods
* search/lookup that can return quotes and news
* download/history periods and intervals, including intraday limits where intraday data cannot extend beyond the last 60 days. ([ranaroussi.github.io][7])

The backend must not pass raw yfinance payloads directly to the frontend or model. It must normalize them.

---

### 8) Normalized domain objects

The system should define stable internal models instead of leaking provider payloads.

#### `TickerOverview`

Fields:

* `symbol`
* `display_name`
* `quote_type`
* `exchange`
* `currency`
* `sector`
* `industry`
* `website`
* `summary`
* `current_price`
* `previous_close`
* `open_price`
* `day_low`
* `day_high`
* `fifty_two_week_low`
* `fifty_two_week_high`
* `volume`
* `average_volume`
* `market_cap`
* `trailing_pe`
* `forward_pe`
* `dividend_yield`
* `beta`
* `shares_outstanding`
* `analyst_target_mean`
* `earnings_date`
* `is_etf`

#### `PriceBar`

Fields:

* `timestamp`
* `open`
* `high`
* `low`
* `close`
* `adj_close`
* `volume`

#### `TickerNewsItem`

Fields:

* `title`
* `publisher`
* `link`
* `published_at`
* `summary`
* `source_type`

#### `FinancialSummary`

Fields:

* `revenue_ttm`
* `net_income_ttm`
* `ebitda`
* `gross_margins`
* `operating_margins`
* `profit_margins`
* `free_cash_flow`
* `total_cash`
* `total_debt`
* `debt_to_equity`
* `return_on_equity`
* `return_on_assets`

#### `TickerAIContext`

Fields:

* `overview`
* `history_summary`
* `financial_summary`
* `news_digest`
* `data_limitations`

The exact fields may vary based on yfinance availability, but the response contracts must stay stable and use `null` for unavailable fields rather than changing shape.

---

### 9) History rules

The history endpoint must support period/interval combinations that are valid for yfinance and reject obviously invalid combinations at the API boundary. yfinance documents valid periods such as `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, and `max`, and valid intervals such as `1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`, `5d`, `1wk`, `1mo`, and `3mo`. Intraday data cannot extend beyond the last 60 days. ([ranaroussi.github.io][8])

The backend should expose only a curated subset to keep frontend logic simple:

* periods: `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `5y`, `max`
* intervals: `1m`, `5m`, `15m`, `1h`, `1d`, `1wk`, `1mo`

Validation must reject combinations that violate the provider’s known history constraints.

---

### 10) Chat grounding design

The chat system must use **tool-based grounding**, not giant prompt stuffing.

The orchestration pattern is:

1. frontend sends symbol + message
2. backend creates a ticker-scoped chat request
3. LLM receives system instruction + conversation + available tools
4. model chooses tools via function calling
5. backend executes tool functions against normalized services
6. backend returns tool results to model
7. model produces final structured answer
8. backend returns clean response to frontend

Tool list for V1:

* `get_stock_snapshot(symbol)`
* `get_price_history(symbol, period, interval)`
* `get_news_context(symbol, limit)`
* `get_financial_summary(symbol)`
* `get_earnings_context(symbol)`
* `get_analyst_context(symbol)`

The model should not receive unrestricted access to the whole backend. Only these controlled, ticker-scoped tools.

Gemini’s function-calling docs explicitly support this style, and its structured output mode supports JSON-schema-constrained results. ([Google AI for Developers][6])

---

### 11) LLM provider interface

Define a provider interface such as:

```python
class LLMProvider(Protocol):
    async def generate_structured(
        self,
        *,
        system_instruction: str,
        messages: list[ChatMessage],
        tools: list[ToolSpec],
        response_schema: dict,
    ) -> StructuredLLMResponse:
        ...

    async def stream_structured(
        self,
        *,
        system_instruction: str,
        messages: list[ChatMessage],
        tools: list[ToolSpec],
        response_schema: dict,
    ) -> AsyncIterator[StreamChunk]:
        ...
```

Concrete implementations:

* `GeminiProvider`
* `OpenAICompatProvider`

`GeminiProvider` uses the official Google Gen AI Python SDK. Google’s quickstart documents the Python SDK package as `google-genai`, and Gemini docs support both structured outputs and function calling. ([Google AI for Developers][9])

`OpenAICompatProvider` is optional and intended mainly for LM Studio local testing. LM Studio documents OpenAI-compatible endpoints where an OpenAI client can be pointed at `http://localhost:1234/v1`, and it also supports OpenAI-compatible tool/function and structured output flows. ([LM Studio][10])

---

### 12) API routes

Base path: `/api/v1`

#### `GET /health`

Returns liveness.

Response:

```json
{
  "status": "ok"
}
```

#### `GET /tickers/search?q=apple`

Searches symbols/names for user entry assistance.

Response:

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

This may be powered by yfinance lookup/search capability. ([ranaroussi.github.io][11])

#### `GET /tickers/{symbol}`

Returns normalized overview for the ticker.

Response:

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
    "summary": "...",
    "current_price": 0,
    "market_cap": 0
  },
  "dataLimitations": []
}
```

#### `GET /tickers/{symbol}/history?period=6mo&interval=1d`

Returns chart-ready history.

Response:

```json
{
  "symbol": "AAPL",
  "period": "6mo",
  "interval": "1d",
  "bars": [
    {
      "timestamp": "2026-03-01T00:00:00Z",
      "open": 0,
      "high": 0,
      "low": 0,
      "close": 0,
      "adj_close": 0,
      "volume": 0
    }
  ]
}
```

#### `GET /tickers/{symbol}/news?limit=10`

Returns normalized ticker news.

#### `GET /tickers/{symbol}/financial-summary`

Returns compact normalized financial summary.

#### `POST /chat`

Request:

```json
{
  "symbol": "AAPL",
  "message": "Why has this stock moved recently?",
  "conversation": [
    { "role": "user", "content": "..." }
  ]
}
```

Response:

```json
{
  "symbol": "AAPL",
  "answer": "Recent movement appears to be driven by ...",
  "highlights": [
    "Shares rose over the selected period",
    "Recent news mentions ..."
  ],
  "usedTools": [
    "get_stock_snapshot",
    "get_news_context"
  ],
  "limitations": [
    "News coverage is limited to the current data provider."
  ]
}
```

#### `POST /analytics/events`

Records anonymous frontend events.

Request:

```json
{
  "symbol": "AAPL",
  "eventType": "view",
  "sessionId": "anon-123"
}
```

#### `GET /analytics/popular?window=24h&limit=10`

Returns popular stocks based on recorded events.

---

### 13) Chat response schema

The final AI response returned to the frontend must be structured.

Recommended schema:

* `answer: string`
* `highlights: string[]`
* `usedTools: string[]`
* `limitations: string[]`

That structure is deliberate:

* `answer` renders the main text
* `highlights` supports quick bullet UI
* `usedTools` aids debugging and trust
* `limitations` keeps the app honest

This maps directly onto Gemini structured outputs. ([Google AI for Developers][12])

---

### 14) Prompting/system instruction

Use a strict system instruction along these lines:

* You are a stock analysis assistant for a single active ticker.
* You may only answer using tool results returned by the backend.
* Never invent numbers, events, news, or metrics.
* If data is missing, say so directly.
* Explain uncertainty.
* Do not provide personalized investment advice.
* Keep answers useful, concise, and grounded in the supplied data.

This instruction belongs in the provider call layer, not in route handlers.

---

### 15) SQLite analytics design

We do not store accounts or chat threads.

We only store lightweight anonymous usage events to compute popularity.

Recommended table:

`analytics_events`

* `id`
* `symbol`
* `event_type`
* `session_id`
* `created_at`

Allowed event types:

* `search`
* `view`
* `chat_opened`
* `chat_message`

SQLite is suitable here because it is file-based, serverless, and embedded; Python’s standard library includes `sqlite3`, which is enough for a project this size. ([SQLite][2])

Popularity should be computed from event aggregates, for example:

* score = weighted count over a time window
* `view` weight > `search`
* optional recency decay later, not required for V1

No per-user identity is needed beyond an optional anonymous `session_id`.

---

### 16) Configuration

Use environment-driven config via Pydantic settings.

Required env vars:

* `APP_ENV`
* `APP_NAME`
* `API_PREFIX=/api/v1`
* `GEMINI_API_KEY`
* `LLM_PROVIDER=gemini`
* `GEMINI_MODEL=...`
* `SQLITE_DB_PATH=./data/app.db`
* `ALLOWED_ORIGINS=http://localhost:3000`

Optional env vars:

* `OPENAI_COMPAT_BASE_URL=http://localhost:1234/v1`
* `OPENAI_COMPAT_API_KEY=dummy`
* `OPENAI_COMPAT_MODEL=...`
* `CACHE_TTL_OVERVIEW_SECONDS=300`
* `CACHE_TTL_HISTORY_SECONDS=300`
* `CACHE_TTL_NEWS_SECONDS=900`
* `CACHE_TTL_FINANCIALS_SECONDS=3600`

FastAPI’s settings guidance explicitly supports `.env`-based configuration with cached settings objects. ([fastapi.tiangolo.com][13])

---

### 17) Caching

Add simple in-process caching for expensive read paths.

Cache:

* ticker overview
* history
* news
* financial summary

Do not cache:

* analytics writes
* chat final responses in V1

Reason:

* the same ticker pages will be opened repeatedly
* yfinance calls are relatively expensive and can fail transiently
* chat responses may vary by question and conversation state

A simple TTL cache is enough for V1.

---

### 18) Error handling

All routes must return structured errors.

Use codes such as:

* `INVALID_SYMBOL`
* `NOT_FOUND`
* `DATA_UNAVAILABLE`
* `INVALID_PERIOD_INTERVAL`
* `PROVIDER_ERROR`
* `LLM_ERROR`
* `RATE_LIMITED`

Examples:

* invalid ticker symbol
* history request outside provider constraints
* unavailable financial fields
* Gemini API failure
* local LLM provider unavailable
* malformed tool response

Do not leak raw stack traces.

---

### 19) Non-goals

Codex must not add these unless explicitly asked later:

* auth
* user profiles
* favorites
* watchlists
* persistent chat history
* portfolio tracking
* websocket market streaming
* scraping beyond yfinance
* trade recommendations
* paper trading
* agentic web browsing for stock answers

The AI chat must stay grounded only in backend-provided market/news context for the active ticker.

---

### 20) Build order

Implement in this order:

#### Phase 1

* FastAPI app skeleton
* config/settings
* health route
* modular routers
* base error handling

#### Phase 2

* yfinance service
* symbol lookup/search route
* ticker overview route
* history route
* normalization layer

#### Phase 3

* news route
* financial-summary route
* data limitation handling

#### Phase 4

* SQLite setup
* analytics events write endpoint
* popular stocks read endpoint

#### Phase 5

* LLM provider abstraction
* Gemini provider implementation
* chat tool specs
* chat orchestrator
* `POST /chat`

#### Phase 6

* optional OpenAI-compatible local provider for LM Studio
* optional `POST /chat/stream`

#### Phase 7

* tests
* docs
* cleanup

---

### 21) Acceptance criteria

V1 is complete when all of the following are true:

1. User can search for a ticker and get symbol suggestions. ([ranaroussi.github.io][11])
2. User can open a ticker page and receive normalized overview data from yfinance. ([ranaroussi.github.io][7])
3. User can request chart/history data for valid period/interval combinations. ([ranaroussi.github.io][8])
4. User can fetch recent news for the ticker. ([ranaroussi.github.io][14])
5. User can ask a question in chat and receive a ticker-grounded answer produced through tool use and structured output. ([Google AI for Developers][6])
6. User activity can be recorded anonymously and used to compute popular stocks. ([SQLite][2])
7. No auth is required.
8. Chat history disappears when the client session is gone.
9. The codebase is modular and not a single-file mess. ([fastapi.tiangolo.com][4])

---

### 22) Direct instruction for Codex

Build exactly this backend V1 with:

* FastAPI
* yfinance
* Gemini as the primary LLM provider
* provider abstraction for optional LM Studio/OpenAI-compatible local testing
* SQLite only for anonymous analytics
* no auth
* no persistent chats
* no extra product features outside this spec

Optimize for:

* correctness
* simple architecture
* typed schemas
* small clean services
* honest error handling
* weekend-project speed without sloppy design

If you want, I’ll turn this into an even sharper `spec.md` file with explicit endpoint schemas and exact Python package choices next.

[1]: https://ranaroussi.github.io/yfinance/reference/index.html?utm_source=chatgpt.com "API Reference — yfinance"
[2]: https://sqlite.org/about.html?utm_source=chatgpt.com "About SQLite"
[3]: https://ranaroussi.github.io/yfinance/?utm_source=chatgpt.com "yfinance documentation"
[4]: https://fastapi.tiangolo.com/tutorial/bigger-applications/?utm_source=chatgpt.com "Bigger Applications - Multiple Files"
[5]: https://ranaroussi.github.io/yfinance/reference/yfinance.screener.html?utm_source=chatgpt.com "Screener & Query — yfinance"
[6]: https://ai.google.dev/gemini-api/docs/function-calling?utm_source=chatgpt.com "Function calling with the Gemini API | Google AI for Developers"
[7]: https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.html?utm_source=chatgpt.com "Ticker — yfinance"
[8]: https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html?utm_source=chatgpt.com "yfinance.download"
[9]: https://ai.google.dev/gemini-api/docs/quickstart?utm_source=chatgpt.com "Gemini API quickstart | Google AI for Developers"
[10]: https://lmstudio.ai/docs/developer/openai-compat?utm_source=chatgpt.com "OpenAI Compatibility Endpoints | LM Studio Docs"
[11]: https://ranaroussi.github.io/yfinance/reference/yfinance.search.html?utm_source=chatgpt.com "Search & Lookup — yfinance"
[12]: https://ai.google.dev/gemini-api/docs/structured-output?utm_source=chatgpt.com "Structured outputs | Gemini API - Google AI for Developers"
[13]: https://fastapi.tiangolo.com/advanced/settings/?utm_source=chatgpt.com "Settings and Environment Variables"
[14]: https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.get_news.html?utm_source=chatgpt.com "yfinance.Ticker.get_news"
