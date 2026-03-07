from pydantic import BaseModel, Field


class AnalyticsEventIngestRequest(BaseModel):
    symbol: str
    eventType: str
    sessionId: str | None = None


class AnalyticsEventIngestResponse(BaseModel):
    accepted: bool = True
    symbol: str
    eventType: str
    sessionId: str | None = None
    recordedAt: str


class PopularTicker(BaseModel):
    symbol: str
    score: int
    totalEvents: int
    searchEvents: int
    viewEvents: int
    chatOpenedEvents: int
    chatMessageEvents: int


class PopularTickersResponse(BaseModel):
    window: str
    limit: int
    generatedAt: str
    results: list[PopularTicker] = Field(default_factory=list)
