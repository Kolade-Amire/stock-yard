from typing import Literal

from pydantic import BaseModel, Field


class ChatTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1)


class ChatRequest(BaseModel):
    symbol: str
    message: str = Field(min_length=1)
    conversation: list[ChatTurn] = Field(default_factory=list)


class ChatResponse(BaseModel):
    symbol: str
    answer: str
    highlights: list[str] = Field(default_factory=list)
    usedTools: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
