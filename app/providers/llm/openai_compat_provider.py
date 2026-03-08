from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from starlette.concurrency import run_in_threadpool

from app.core.errors import ApiError
from app.core.logging import get_logger
from app.providers.llm.base import LLMMessage, LLMModelResponse, LLMProvider, ToolCall, ToolSpec


class OpenAICompatProvider(LLMProvider):
    def __init__(self, *, base_url: str, api_key: str, model: str) -> None:
        self._base_url = base_url
        self._api_key = api_key
        self._model = model
        self._logger = get_logger(__name__)

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
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ApiError(
                code="LLM_ERROR",
                message="OpenAI-compatible provider is not installed.",
                status_code=502,
                details={"provider": "openai_compat"},
            ) from exc

        client = OpenAI(base_url=self._base_url, api_key=self._api_key)
        payload: dict[str, Any] = {
            "model": self._model,
            "messages": self._to_openai_messages(system_instruction, messages),
            "tools": self._to_openai_tools(tools),
            "tool_choice": "auto" if tools else None,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "chat_response",
                    "schema": response_schema,
                },
            },
        }
        payload = {key: value for key, value in payload.items() if value is not None}

        try:
            completion = client.chat.completions.create(**payload)
        except Exception as exc:
            lowered = str(exc).lower()
            if "response_format" in lowered or "json_schema" in lowered:
                self._logger.warning(
                    "OpenAI-compatible endpoint rejected response_format; retrying without json schema."
                )
                payload.pop("response_format", None)
                try:
                    completion = client.chat.completions.create(**payload)
                except Exception as nested_exc:
                    raise ApiError(
                        code="LLM_ERROR",
                        message="OpenAI-compatible provider request failed.",
                        status_code=502,
                        details={"provider": "openai_compat"},
                    ) from nested_exc
            else:
                raise ApiError(
                    code="LLM_ERROR",
                    message="OpenAI-compatible provider request failed.",
                    status_code=502,
                    details={"provider": "openai_compat"},
                ) from exc

        if not completion.choices:
            raise ApiError(
                code="LLM_ERROR",
                message="OpenAI-compatible provider returned no choices.",
                status_code=502,
                details={"provider": "openai_compat"},
            )

        message = completion.choices[0].message
        if message.tool_calls:
            tool_calls: list[ToolCall] = []
            for raw_tool_call in message.tool_calls:
                name = raw_tool_call.function.name
                arguments = self._parse_tool_arguments(raw_tool_call.function.arguments)
                tool_calls.append(
                    ToolCall(
                        id=raw_tool_call.id or f"call_{uuid4().hex}",
                        name=name,
                        arguments=arguments,
                    )
                )
            return LLMModelResponse(tool_calls=tool_calls)

        text = self._extract_message_text(message.content)
        parsed = self._parse_json_text(text)
        return LLMModelResponse(text=text, parsed=parsed)

    @staticmethod
    def _to_openai_messages(
            system_instruction: str,
        messages: list[LLMMessage],
    ) -> list[dict[str, Any]]:
        serialized_messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_instruction}
        ]

        for message in messages:
            if message.role == "tool":
                serialized_messages.append(
                    {
                        "role": "tool",
                        "content": message.content,
                        "name": message.name,
                        "tool_call_id": message.tool_call_id,
                    }
                )
                continue

            serialized: dict[str, Any] = {
                "role": message.role,
                "content": message.content,
            }
            if message.tool_calls:
                serialized["tool_calls"] = [
                    {
                        "id": tool_call.id,
                        "type": "function",
                        "function": {
                            "name": tool_call.name,
                            "arguments": json.dumps(tool_call.arguments),
                        },
                    }
                    for tool_call in message.tool_calls
                ]
            serialized_messages.append(serialized)

        return serialized_messages

    @staticmethod
    def _to_openai_tools(tools: list[ToolSpec]) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                },
            }
            for tool in tools
        ]

    @staticmethod
    def _parse_tool_arguments(raw_arguments: str | None) -> dict[str, Any]:
        if not raw_arguments:
            return {}
        try:
            parsed = json.loads(raw_arguments)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _extract_message_text(content: Any) -> str:
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            texts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str):
                        texts.append(text)
            return "\n".join(texts).strip()
        return str(content)

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
