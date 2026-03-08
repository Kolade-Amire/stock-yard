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


class PriceBar(BaseModel):
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    adj_close: float | None = None
    volume: int | None = None


class TickerHistoryResponse(BaseModel):
    symbol: str
    period: str
    interval: str
    bars: list[PriceBar] = Field(default_factory=list)


class TickerNewsItem(BaseModel):
    title: str | None = None
    publisher: str | None = None
    link: str | None = None
    published_at: str | None = None
    summary: str | None = None
    source_type: str | None = None


class TickerNewsResponse(BaseModel):
    symbol: str
    news: list[TickerNewsItem] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class FinancialSummary(BaseModel):
    revenue_ttm: float | None = None
    net_income_ttm: float | None = None
    ebitda: float | None = None
    gross_margins: float | None = None
    operating_margins: float | None = None
    profit_margins: float | None = None
    free_cash_flow: float | None = None
    total_cash: float | None = None
    total_debt: float | None = None
    debt_to_equity: float | None = None
    return_on_equity: float | None = None
    return_on_assets: float | None = None


class FinancialSummaryResponse(BaseModel):
    symbol: str
    financialSummary: FinancialSummary
    dataLimitations: list[str] = Field(default_factory=list)


class EarningsContext(BaseModel):
    next_earnings_date: str | None = None
    earnings_date_candidates: list[str] = Field(default_factory=list)
    eps_estimate_low: float | None = None
    eps_estimate_avg: float | None = None
    eps_estimate_high: float | None = None
    revenue_estimate_low: float | None = None
    revenue_estimate_avg: float | None = None
    revenue_estimate_high: float | None = None
    data_sources: list[str] = Field(default_factory=list)


class EarningsContextResponse(BaseModel):
    symbol: str
    earningsContext: EarningsContext
    dataLimitations: list[str] = Field(default_factory=list)


class AnalystRecommendationSnapshot(BaseModel):
    period: str | None = None
    strong_buy: int | None = None
    buy: int | None = None
    hold: int | None = None
    sell: int | None = None
    strong_sell: int | None = None


class AnalystActionEvent(BaseModel):
    graded_at: str | None = None
    firm: str | None = None
    to_grade: str | None = None
    from_grade: str | None = None
    action: str | None = None
    price_target_action: str | None = None
    current_price_target: float | None = None
    prior_price_target: float | None = None


class AnalystContext(BaseModel):
    current_price_target: float | None = None
    target_low: float | None = None
    target_high: float | None = None
    target_mean: float | None = None
    target_median: float | None = None
    recommendation_summary: AnalystRecommendationSnapshot = Field(
        default_factory=AnalystRecommendationSnapshot
    )
    recent_actions: list[AnalystActionEvent] = Field(default_factory=list)
    recent_action_count: int = 0
    recent_action_window_days: int = 0


class AnalystContextResponse(BaseModel):
    symbol: str
    analystContext: AnalystContext
    dataLimitations: list[str] = Field(default_factory=list)
