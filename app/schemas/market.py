from pydantic import BaseModel, ConfigDict, Field


class MarketMover(BaseModel):
    symbol: str
    name: str | None = None
    exchange: str | None = None
    quoteType: str | None = None
    currentPrice: float | None = None
    change: float | None = None
    percentChange: float | None = None
    volume: int | None = None
    marketCap: float | None = None


class MarketMoversResponse(BaseModel):
    screen: str
    marketScope: str
    asOf: str
    results: list[MarketMover] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class BenchmarkHolding(BaseModel):
    symbol: str
    name: str | None = None
    holdingPercent: float | None = None


class BenchmarkSectorWeight(BaseModel):
    sector: str
    weight: float | None = None


class BenchmarkFund(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    symbol: str
    benchmarkKey: str
    benchmarkName: str
    category: str
    displayName: str | None = None
    currentPrice: float | None = None
    previousClose: float | None = None
    dayChange: float | None = None
    dayChangePercent: float | None = None
    currency: str | None = None
    expenseRatio: float | None = None
    netAssets: float | None = None
    yield_: float | None = Field(default=None, alias="yield")
    fundFamily: str | None = None
    topHoldings: list[BenchmarkHolding] = Field(default_factory=list)
    sectorWeights: list[BenchmarkSectorWeight] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class BenchmarkFundsResponse(BaseModel):
    asOf: str
    funds: list[BenchmarkFund] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class EarningsCalendarEvent(BaseModel):
    symbol: str
    companyName: str | None = None
    earningsDate: str
    reportTime: str | None = None
    epsEstimate: float | None = None
    reportedEps: float | None = None
    surprisePercent: float | None = None
    marketCap: float | None = None


class EarningsCalendarResponse(BaseModel):
    start: str
    end: str
    limit: int
    offset: int
    activeOnly: bool
    returnedCount: int
    hasMore: bool
    nextOffset: int | None = None
    events: list[EarningsCalendarEvent] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class SectorOverview(BaseModel):
    companiesCount: int | None = None
    marketCap: float | None = None
    messageBoardId: str | None = None
    description: str | None = None
    industriesCount: int | None = None
    marketWeight: float | None = None
    employeeCount: int | None = None


class SectorFundReference(BaseModel):
    symbol: str
    name: str | None = None


class SectorCompanyReference(BaseModel):
    symbol: str
    name: str | None = None
    rating: str | None = None
    marketWeight: float | None = None


class SectorIndustryReference(BaseModel):
    key: str
    name: str | None = None
    symbol: str | None = None
    marketWeight: float | None = None


class SectorPulseItem(BaseModel):
    key: str
    name: str | None = None
    symbol: str | None = None
    overview: SectorOverview
    topEtfs: list[SectorFundReference] = Field(default_factory=list)
    topMutualFunds: list[SectorFundReference] = Field(default_factory=list)
    topCompanies: list[SectorCompanyReference] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class SectorPulseResponse(BaseModel):
    asOf: str
    sectors: list[SectorPulseItem] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class SectorDetailResponse(BaseModel):
    key: str
    name: str | None = None
    symbol: str | None = None
    overview: SectorOverview
    topEtfs: list[SectorFundReference] = Field(default_factory=list)
    topMutualFunds: list[SectorFundReference] = Field(default_factory=list)
    topCompanies: list[SectorCompanyReference] = Field(default_factory=list)
    industries: list[SectorIndustryReference] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)


class IndustryOverview(BaseModel):
    companiesCount: int | None = None
    marketCap: float | None = None
    messageBoardId: str | None = None
    description: str | None = None
    marketWeight: float | None = None
    employeeCount: int | None = None


class IndustryCompanyReference(BaseModel):
    symbol: str
    name: str | None = None
    rating: str | None = None
    marketWeight: float | None = None


class IndustryGrowthCompanyReference(BaseModel):
    symbol: str
    name: str | None = None
    ytdReturn: float | None = None
    growthEstimate: float | None = None


class IndustryPerformingCompanyReference(BaseModel):
    symbol: str
    name: str | None = None
    ytdReturn: float | None = None
    lastPrice: float | None = None
    targetPrice: float | None = None


class IndustryDetailResponse(BaseModel):
    key: str
    name: str | None = None
    symbol: str | None = None
    sectorKey: str | None = None
    sectorName: str | None = None
    overview: IndustryOverview
    topCompanies: list[IndustryCompanyReference] = Field(default_factory=list)
    topGrowthCompanies: list[IndustryGrowthCompanyReference] = Field(default_factory=list)
    topPerformingCompanies: list[IndustryPerformingCompanyReference] = Field(default_factory=list)
    dataLimitations: list[str] = Field(default_factory=list)
