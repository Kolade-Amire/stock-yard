from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(examples=["ok"])


class ErrorBody(BaseModel):
    code: str = Field(examples=["NOT_FOUND"])
    message: str = Field(examples=["Resource not found."])
    details: dict[str, Any] | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody
