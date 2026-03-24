import unittest
from unittest.mock import MagicMock, patch

from openai.types.chat.chat_completion import ChatCompletion

from app.providers.llm.base import ToolSpec
from app.providers.llm.openai_compat_provider import OpenAICompatProvider


CHAT_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "highlights": {
            "type": "array",
            "items": {"type": "string"},
        },
        "limitations": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["answer", "highlights", "limitations"],
    "additionalProperties": False,
}


class OpenAICompatProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = OpenAICompatProvider(
            base_url="http://127.0.0.1:1234/v1",
            api_key="dummy",
            model="qwen/qwen3.5-9b:2",
        )

    def _build_completion(
        self,
        *,
        content: str | None,
        reasoning_content: str | None = None,
        tool_calls: list[dict[str, object]] | None = None,
        finish_reason: str = "stop",
    ) -> ChatCompletion:
        message: dict[str, object] = {
            "role": "assistant",
            "content": content,
            "tool_calls": tool_calls or [],
        }
        if reasoning_content is not None:
            message["reasoning_content"] = reasoning_content

        payload = {
            "id": "chatcmpl_test",
            "object": "chat.completion",
            "created": 1,
            "model": "qwen/qwen3.5-9b:2",
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "finish_reason": finish_reason,
                    "logprobs": None,
                }
            ],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }
        return ChatCompletion.model_validate(payload)

    def _generate_sync_with_completion(
        self,
        completion: ChatCompletion,
        *,
        tools: list[ToolSpec] | None = None,
    ) -> tuple[object, MagicMock]:
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = completion

        with patch("openai.OpenAI", return_value=mock_client):
            response = self.provider._generate_sync(
                system_instruction="Return only valid JSON that matches the schema.",
                messages=[],
                tools=tools or [],
                response_schema=CHAT_RESPONSE_SCHEMA,
            )

        return response, mock_client

    def test_generate_sync_parses_json_from_content(self) -> None:
        json_text = '{"answer":"ok","highlights":["from-content"],"limitations":[]}'
        completion = self._build_completion(content=json_text)

        response, mock_client = self._generate_sync_with_completion(completion)

        self.assertEqual(response.text, json_text)
        self.assertEqual(
            response.parsed,
            {
                "answer": "ok",
                "highlights": ["from-content"],
                "limitations": [],
            },
        )
        self.assertEqual(response.tool_calls, [])
        self.assertEqual(mock_client.chat.completions.create.call_count, 1)

    def test_generate_sync_falls_back_to_reasoning_content_when_content_is_empty(self) -> None:
        reasoning_json = '{"answer":"ok","highlights":["from-reasoning"],"limitations":[]}'
        completion = self._build_completion(content="", reasoning_content=reasoning_json)

        with self.assertLogs("app.providers.llm.openai_compat_provider", level="INFO") as captured:
            response, _ = self._generate_sync_with_completion(completion)

        self.assertEqual(response.text, reasoning_json)
        self.assertEqual(
            response.parsed,
            {
                "answer": "ok",
                "highlights": ["from-reasoning"],
                "limitations": [],
            },
        )
        self.assertIn(
            "response_source=reasoning_content",
            "\n".join(captured.output),
        )

    def test_generate_sync_falls_back_to_reasoning_content_when_content_is_not_json(self) -> None:
        reasoning_json = '{"answer":"ok","highlights":["fallback"],"limitations":[]}'
        completion = self._build_completion(
            content="Here is the answer in prose.",
            reasoning_content=reasoning_json,
        )

        response, _ = self._generate_sync_with_completion(completion)

        self.assertEqual(response.text, reasoning_json)
        self.assertEqual(
            response.parsed,
            {
                "answer": "ok",
                "highlights": ["fallback"],
                "limitations": [],
            },
        )

    def test_generate_sync_returns_tool_calls_during_tool_selection(self) -> None:
        completion = self._build_completion(
            content=None,
            tool_calls=[
                {
                    "id": "call_1",
                    "type": "function",
                    "function": {
                        "name": "get_stock_snapshot",
                        "arguments": '{"period":"1d"}',
                    },
                }
            ],
            finish_reason="tool_calls",
        )

        response, mock_client = self._generate_sync_with_completion(
            completion,
            tools=[
                ToolSpec(
                    name="get_stock_snapshot",
                    description="Fetches stock snapshot data.",
                    parameters={
                        "type": "object",
                        "properties": {
                            "period": {"type": "string"},
                        },
                        "additionalProperties": False,
                    },
                )
            ],
        )

        self.assertIsNone(response.text)
        self.assertIsNone(response.parsed)
        self.assertEqual(len(response.tool_calls), 1)
        self.assertEqual(response.tool_calls[0].name, "get_stock_snapshot")
        self.assertEqual(response.tool_calls[0].arguments, {"period": "1d"})
        self.assertEqual(mock_client.chat.completions.create.call_count, 1)


if __name__ == "__main__":
    unittest.main()
