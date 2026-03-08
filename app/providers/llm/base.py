from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class LLMMessage:
    role: Literal["user", "assistant", "tool"]
    content: str
    name: str | None = None
    tool_call_id: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)


@dataclass(frozen=True)
class LLMModelResponse:
    text: str | None = None
    parsed: dict[str, Any] | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)


class LLMProvider(ABC):
    @abstractmethod
    async def generate(
        self,
        *,
        system_instruction: str,
        messages: list[LLMMessage],
        tools: list[ToolSpec],
        response_schema: dict[str, Any],
    ) -> LLMModelResponse:
        ...
