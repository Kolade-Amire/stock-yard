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


class FinancialTrendPoint(BaseModel):
    periodEnd: str
    revenue: float | None = None
    netIncome: float | None = None
    operatingCashFlow: float | None = None
    capitalExpenditure: float | None = None
    freeCashFlow: float | None = None


class FinancialTrendsResponse(BaseModel):
    symbol: str
    annual: list[FinancialTrendPoint] = Field(default_factory=list)
    quarterly: list[FinancialTrendPoint] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class EarningsHistoryEvent(BaseModel):
    reportDate: str
    quarter: str | None = None
    epsEstimate: float | None = None
    epsActual: float | None = None
    surprisePercent: float | None = None


class EarningsHistoryResponse(BaseModel):
    symbol: str
    events: list[EarningsHistoryEvent] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class EarningsEstimatePoint(BaseModel):
    period: str
    avg: float | None = None
    low: float | None = None
    high: float | None = None
    yearAgoEps: float | None = None
    numberOfAnalysts: int | None = None
    growth: float | None = None


class RevenueEstimatePoint(BaseModel):
    period: str
    avg: float | None = None
    low: float | None = None
    high: float | None = None
    numberOfAnalysts: int | None = None
    yearAgoRevenue: float | None = None
    growth: float | None = None


class GrowthEstimatePoint(BaseModel):
    period: str
    stockTrend: float | None = None
    indexTrend: float | None = None


class EarningsEstimatesResponse(BaseModel):
    symbol: str
    epsEstimates: list[EarningsEstimatePoint] = Field(default_factory=list)
    revenueEstimates: list[RevenueEstimatePoint] = Field(default_factory=list)
    growthEstimates: list[GrowthEstimatePoint] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class ComparisonSeriesItem(BaseModel):
    symbol: str
    displayName: str | None = None
    currentPrice: float | None = None
    changePercent: float | None = None
    bars: list[PriceBar] = Field(default_factory=list)


class TickerCompareResponse(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    period: str
    interval: str
    series: list[ComparisonSeriesItem] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


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


class AnalystRecommendationBreakdown(BaseModel):
    period: str | None = None
    strongBuy: int | None = None
    buy: int | None = None
    hold: int | None = None
    sell: int | None = None
    strongSell: int | None = None


class AnalystActionTimelineEvent(BaseModel):
    gradedAt: str | None = None
    firm: str | None = None
    toGrade: str | None = None
    fromGrade: str | None = None
    action: str | None = None
    priceTargetAction: str | None = None
    currentPriceTarget: float | None = None
    priorPriceTarget: float | None = None


class AnalystSummary(BaseModel):
    currentPriceTarget: float | None = None
    targetLow: float | None = None
    targetHigh: float | None = None
    targetMean: float | None = None
    targetMedian: float | None = None
    recommendationSummary: AnalystRecommendationBreakdown = Field(
        default_factory=AnalystRecommendationBreakdown
    )
    recentActionCount: int = 0
    recentActionWindowDays: int = 0


class AnalystSummaryResponse(BaseModel):
    symbol: str
    analystSummary: AnalystSummary
    dataLimitations: list[str] = Field(default_factory=list)


class AnalystHistoryResponse(BaseModel):
    symbol: str
    recommendationHistory: list[AnalystRecommendationBreakdown] = Field(default_factory=list)
    actions: list[AnalystActionTimelineEvent] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class MajorHolderMetric(BaseModel):
    key: str
    label: str
    value: float | None = None


class HolderEntry(BaseModel):
    dateReported: str | None = None
    holder: str | None = None
    pctHeld: float | None = None
    shares: int | None = None
    value: float | None = None
    pctChange: float | None = None


class InsiderRosterEntry(BaseModel):
    name: str | None = None
    position: str | None = None
    url: str | None = None
    mostRecentTransaction: str | None = None
    latestTransactionDate: str | None = None
    sharesOwnedDirectly: int | None = None
    positionDirectDate: str | None = None


class OwnershipPagination(BaseModel):
    offset: int
    limit: int
    returnedCount: int
    totalAvailable: int
    hasMore: bool
    nextOffset: int | None = None


class OwnershipResponse(BaseModel):
    symbol: str
    requestedSection: str
    limit: int
    offset: int
    majorHolders: list[MajorHolderMetric] = Field(default_factory=list)
    institutionalHolders: list[HolderEntry] = Field(default_factory=list)
    mutualFundHolders: list[HolderEntry] = Field(default_factory=list)
    insiderRoster: list[InsiderRosterEntry] = Field(default_factory=list)
    institutionalPagination: OwnershipPagination | None = None
    mutualFundPagination: OwnershipPagination | None = None
    insiderRosterPagination: OwnershipPagination | None = None
    dataLimitations: list[str] = Field(default_factory=list)


class OptionsExpirationsResponse(BaseModel):
    symbol: str
    expirations: list[str] = Field(default_factory=list)


class OptionContract(BaseModel):
    contractSymbol: str
    lastTradeDate: str | None = None
    strike: float | None = None
    lastPrice: float | None = None
    bid: float | None = None
    ask: float | None = None
    change: float | None = None
    percentChange: float | None = None
    volume: int | None = None
    openInterest: int | None = None
    impliedVolatility: float | None = None
    inTheMoney: bool | None = None
    contractSize: str | None = None
    currency: str | None = None


class OptionsChainResponse(BaseModel):
    symbol: str
    expiration: str
    underlyingPrice: float | None = None
    calls: list[OptionContract] = Field(default_factory=list)
    puts: list[OptionContract] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)
