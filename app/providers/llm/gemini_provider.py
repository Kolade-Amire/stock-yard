from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from starlette.concurrency import run_in_threadpool

from app.core.errors import ApiError
from app.providers.llm.base import LLMMessage, LLMModelResponse, LLMProvider, ToolCall, ToolSpec


class GeminiProvider(LLMProvider):
    def __init__(self, *, api_key: str | None, model: str) -> None:
        self._api_key = api_key
        self._model = model

    async def generate(
        self,
        *,
        system_instruction: str,
        messages: list[LLMMessage],
        tools: list[ToolSpec],
        response_schema: dict[str, Any],
    ) -> LLMModelResponse:
        return await run_in_threadpool(
            self._generate_sync,
            system_instruction,
            messages,
            tools,
            response_schema,
        )

    def _generate_sync(
        self,
        system_instruction: str,
        messages: list[LLMMessage],
        tools: list[ToolSpec],
        response_schema: dict[str, Any],
    ) -> LLMModelResponse:
        if not self._api_key:
            raise ApiError(
                code="LLM_ERROR",
                message="GEMINI_API_KEY is not configured.",
                status_code=502,
                details={"provider": "gemini"},
            )

        try:
            from google import genai
        except ImportError as exc:
            raise ApiError(
                code="LLM_ERROR",
                message="Gemini provider SDK is not installed.",
                status_code=502,
                details={"provider": "gemini"},
            ) from exc

        client = genai.Client(api_key=self._api_key)
        config = {
            "system_instruction": system_instruction,
            "tools": [
                {
                    "function_declarations": [
                        {
                            "name": tool.name,
                            "description": tool.description,
                            "parameters": tool.parameters,
                        }
                        for tool in tools
                    ]
                }
            ],
            "response_mime_type": "application/json",
            "response_schema": response_schema,
        }

        try:
            response = client.models.generate_content(
                model=self._model,
                contents=self._to_gemini_contents(messages),
                config=config,
            )
        except Exception as exc:
            raise ApiError(
                code="LLM_ERROR",
                message="Gemini provider request failed.",
                status_code=502,
                details={"provider": "gemini"},
            ) from exc

        tool_calls = self._extract_tool_calls(response)
        if tool_calls:
            return LLMModelResponse(tool_calls=tool_calls)

        text = self._extract_text(response)
        parsed = self._parse_json_text(text)
        return LLMModelResponse(text=text, parsed=parsed)

    def _to_gemini_contents(self, messages: list[LLMMessage]) -> list[dict[str, Any]]:
        contents: list[dict[str, Any]] = []
        for message in messages:
            if message.role == "tool":
                tool_payload = self._safe_json_loads(message.content)
                contents.append(
                    {
                        "role": "user",
                        "parts": [
                            {
                                "function_response": {
                                    "name": message.name or "tool",
                                    "response": tool_payload,
                                }
                            }
                        ],
                    }
                )
                continue

            role = "model" if message.role == "assistant" else "user"
            parts: list[dict[str, Any]] = []
            if message.content:
                parts.append({"text": message.content})

            if message.tool_calls:
                parts.extend(
                    {
                        "function_call": {
                            "name": tool_call.name,
                            "args": tool_call.arguments,
                        }
                    }
                    for tool_call in message.tool_calls
                )

            if not parts:
                parts.append({"text": ""})

            contents.append({"role": role, "parts": parts})
        return contents

    @staticmethod
    def _extract_tool_calls(response: Any) -> list[ToolCall]:
        raw_calls = getattr(response, "function_calls", None)
        if not raw_calls:
            raw_calls = []
            candidates = getattr(response, "candidates", None) or []
            for candidate in candidates:
                content = getattr(candidate, "content", None)
                parts = getattr(content, "parts", None) or []
                for part in parts:
                    function_call = getattr(part, "function_call", None)
                    if function_call is not None:
                        raw_calls.append(function_call)

        tool_calls: list[ToolCall] = []
        for raw_call in raw_calls:
            name = getattr(raw_call, "name", None)
            if not isinstance(name, str) or not name:
                continue
            args = getattr(raw_call, "args", None)
            if not isinstance(args, dict):
                args = {}
            call_id = getattr(raw_call, "id", None) or f"call_{uuid4().hex}"
            tool_calls.append(ToolCall(id=call_id, name=name, arguments=args))
        return tool_calls

    @staticmethod
    def _extract_text(response: Any) -> str:
        text = getattr(response, "text", None)
        if isinstance(text, str):
            return text

        texts: list[str] = []
        candidates = getattr(response, "candidates", None) or []
        for candidate in candidates:
            content = getattr(candidate, "content", None)
            parts = getattr(content, "parts", None) or []
            for part in parts:
                part_text = getattr(part, "text", None)
                if isinstance(part_text, str) and part_text:
                    texts.append(part_text)
        return "\n".join(texts).strip()

    @staticmethod
    def _parse_json_text(text: str) -> dict[str, Any] | None:
        stripped = text.strip()
        if not stripped:
            return None
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    @staticmethod
    def _safe_json_loads(value: str) -> dict[str, Any]:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}
        if isinstance(parsed, dict):
            return parsed
        return {"value": parsed}
