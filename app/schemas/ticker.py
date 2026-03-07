from pydantic import BaseModel, Field


class TickerSearchResult(BaseModel):
    symbol: str
    name: str | None = None
    exchange: str | None = None
    quoteType: str | None = None


class TickerSearchResponse(BaseModel):
    query: str
    results: list[TickerSearchResult] = Field(default_factory=list)


class TickerOverview(BaseModel):
    display_name: str | None = None
    quote_type: str | None = None
    exchange: str | None = None
    currency: str | None = None
    sector: str | None = None
    industry: str | None = None
    website: str | None = None
    summary: str | None = None
    current_price: float | None = None
    previous_close: float | None = None
    open_price: float | None = None
    day_low: float | None = None
    day_high: float | None = None
    fifty_two_week_low: float | None = None
    fifty_two_week_high: float | None = None
    volume: int | None = None
    average_volume: int | None = None
    market_cap: float | None = None
    trailing_pe: float | None = None
    forward_pe: float | None = None
    dividend_yield: float | None = None
    beta: float | None = None
    shares_outstanding: int | None = None
    analyst_target_mean: float | None = None
    earnings_date: str | None = None
    is_etf: bool | None = None


class TickerOverviewResponse(BaseModel):
    symbol: str
    overview: TickerOverview
    dataLimitations: list[str] = Field(default_factory=list)
