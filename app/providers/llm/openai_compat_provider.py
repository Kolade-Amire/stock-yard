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
        serialized_messages = self._to_openai_messages(system_instruction, messages)
        serialized_tools = self._to_openai_tools(tools)
        has_prior_tool_results = any(message.role == "tool" for message in messages)
        is_tool_selection_stage = bool(serialized_tools) and not has_prior_tool_results

        # Stage 1: let local models decide on tool calls without schema pressure.
        if is_tool_selection_stage:
            self._logger.info("OpenAI-compatible stage=tool-selection")
            tool_selection_payload = self._build_payload(
                messages=serialized_messages,
                tools=serialized_tools,
                response_schema=None,
            )
            completion = self._request_completion(client=client, payload=tool_selection_payload)
            completion_message = self._extract_choice_message(completion)
            tool_calls = self._extract_tool_calls(completion_message)
            if tool_calls:
                self._logger.info(
                    "OpenAI-compatible stage=tool-selection tool_calls=%d",
                    len(tool_calls),
                )
                return LLMModelResponse(tool_calls=tool_calls)

            self._logger.info(
                "OpenAI-compatible stage=tool-selection tool_calls=0; fallback=structured-final"
            )
            # No tool calls were emitted; force a structured final answer for contract stability.
            structured_final_payload = self._build_payload(
                messages=serialized_messages,
                tools=[],
                response_schema=response_schema,
            )
        else:
            self._logger.info(
                "OpenAI-compatible stage=structured-final has_prior_tool_results=%s",
                has_prior_tool_results,
            )
            structured_final_payload = self._build_payload(
                messages=serialized_messages,
                tools=serialized_tools,
                response_schema=response_schema,
            )

        completion = self._request_completion(client=client, payload=structured_final_payload)
        completion_message = self._extract_choice_message(completion)
        tool_calls = self._extract_tool_calls(completion_message)
        if tool_calls:
            self._logger.info(
                "OpenAI-compatible stage=structured-final tool_calls=%d",
                len(tool_calls),
            )
            return LLMModelResponse(tool_calls=tool_calls)

        text = self._extract_message_text(completion_message.content)
        parsed = self._parse_json_text(text)
        return LLMModelResponse(text=text, parsed=parsed)

    def _build_payload(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        response_schema: dict[str, Any] | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "tools": tools or None,
            "tool_choice": "auto" if tools else None,
            "response_format": (
                {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "chat_response",
                        "schema": response_schema,
                    },
                }
                if response_schema is not None
                else None
            ),
        }
        return {key: value for key, value in payload.items() if value is not None}

    def _request_completion(self, *, client: Any, payload: dict[str, Any]) -> Any:
        try:
            return client.chat.completions.create(**payload)
        except Exception as exc:
            lowered = str(exc).lower()
            can_retry_without_schema = (
                "response_format" in payload
                and ("response_format" in lowered or "json_schema" in lowered)
            )
            if can_retry_without_schema:
                self._logger.warning(
                    "OpenAI-compatible endpoint rejected response_format; retrying without json schema."
                )
                fallback_payload = dict(payload)
                fallback_payload.pop("response_format", None)
                try:
                    return client.chat.completions.create(**fallback_payload)
                except Exception as nested_exc:
                    raise ApiError(
                        code="LLM_ERROR",
                        message="OpenAI-compatible provider request failed.",
                        status_code=502,
                        details={"provider": "openai_compat"},
                    ) from nested_exc

            raise ApiError(
                code="LLM_ERROR",
                message="OpenAI-compatible provider request failed.",
                status_code=502,
                details={"provider": "openai_compat"},
            ) from exc

    @staticmethod
    def _extract_choice_message(completion: Any) -> Any:
        if not completion.choices:
            raise ApiError(
                code="LLM_ERROR",
                message="OpenAI-compatible provider returned no choices.",
                status_code=502,
                details={"provider": "openai_compat"},
            )
        return completion.choices[0].message

    @staticmethod
    def _extract_tool_calls(message: Any) -> list[ToolCall]:
        if not message.tool_calls:
            return []

        tool_calls: list[ToolCall] = []
        for raw_tool_call in message.tool_calls:
            name = raw_tool_call.function.name
            arguments = OpenAICompatProvider._parse_tool_arguments(raw_tool_call.function.arguments)
            tool_calls.append(
                ToolCall(
                    id=raw_tool_call.id or f"call_{uuid4().hex}",
                    name=name,
                    arguments=arguments,
                )
            )
        return tool_calls

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
