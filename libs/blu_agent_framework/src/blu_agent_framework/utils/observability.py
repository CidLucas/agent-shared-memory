"""
blu_agent_framework.utils.observability
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lightweight observability helpers for LLM calls.

Features
--------
- generate_correlation_id(): unique ID for tracing a request end-to-end.
- log_llm_call(): structured log entry BEFORE the LLM call (model, prompt preview).
- log_llm_response(): structured log entry AFTER the LLM call (latency, response preview).
- log_parse_failure(): dedicated WARNING for JSON parse failures — includes correlation_id
  and truncated raw LLM output so debugging is possible without full log noise.

Usage
-----
    from blu_agent_framework.utils.observability import (
        generate_correlation_id,
        log_llm_call,
        log_llm_response,
        log_parse_failure,
    )

    cid = generate_correlation_id()
    log_llm_call(logger, cid, node="parse_intent", model="gpt-4o", prompt_preview=prompt[:200])
    response = await llm.ainvoke(messages)
    log_llm_response(logger, cid, node="parse_intent", latency_ms=elapsed, response_preview=str(response.content)[:200])
"""
from __future__ import annotations

import time
import uuid
import logging
from typing import Any


def generate_correlation_id() -> str:
    """Return a short unique string for correlating LLM call logs end-to-end."""
    return uuid.uuid4().hex[:12]


def log_llm_call(
    logger: logging.Logger,
    correlation_id: str,
    *,
    node: str,
    model: str | None = None,
    prompt_preview: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """Emit a structured INFO log before an LLM call.

    Parameters
    ----------
    logger:          caller's module logger
    correlation_id:  ID returned by generate_correlation_id()
    node:            graph node name (e.g. 'parse_intent')
    model:           model identifier string (optional, may be None)
    prompt_preview:  first N chars of the prompt (caller should truncate)
    extra:           additional key/value pairs merged into the log record
    """
    payload: dict[str, Any] = {
        "event": "llm_call",
        "correlation_id": correlation_id,
        "node": node,
    }
    if model:
        payload["model"] = model
    if prompt_preview:
        payload["prompt_preview"] = prompt_preview[:500]
    if extra:
        payload.update(extra)
    logger.info("[observability] %s", payload)


def log_llm_response(
    logger: logging.Logger,
    correlation_id: str,
    *,
    node: str,
    latency_ms: float | None = None,
    response_preview: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """Emit a structured INFO log after a successful LLM response.

    Parameters
    ----------
    latency_ms:       elapsed time in milliseconds (optional)
    response_preview: first N chars of the response content (caller should truncate)
    """
    payload: dict[str, Any] = {
        "event": "llm_response",
        "correlation_id": correlation_id,
        "node": node,
    }
    if latency_ms is not None:
        payload["latency_ms"] = round(latency_ms, 1)
    if response_preview:
        payload["response_preview"] = response_preview[:500]
    if extra:
        payload.update(extra)
    logger.info("[observability] %s", payload)


def log_parse_failure(
    logger: logging.Logger,
    correlation_id: str,
    *,
    node: str,
    raw_response: str,
    reason: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """Emit a WARNING for a JSON parse failure with a truncated raw response.

    Always includes the correlation_id so callers can trace the original LLM
    call from the INFO logs above.

    Parameters
    ----------
    raw_response: full raw LLM output — truncated to 800 chars before logging.
    reason:       short human description of why parsing failed (optional).
    """
    payload: dict[str, Any] = {
        "event": "parse_failure",
        "correlation_id": correlation_id,
        "node": node,
        "raw_response": raw_response[:800],
    }
    if reason:
        payload["reason"] = reason
    if extra:
        payload.update(extra)
    logger.warning("[observability] %s", payload)


class LLMCallTimer:
    """Context manager that measures LLM call latency in milliseconds.

    Usage::

        with LLMCallTimer() as timer:
            response = await llm.ainvoke(messages)
        log_llm_response(logger, cid, node="x", latency_ms=timer.elapsed_ms, ...)
    """

    def __init__(self) -> None:
        self._start: float = 0.0
        self.elapsed_ms: float = 0.0

    def __enter__(self) -> "LLMCallTimer":
        self._start = time.monotonic()
        return self

    def __exit__(self, *_: Any) -> None:
        self.elapsed_ms = (time.monotonic() - self._start) * 1000

    # async support
    async def __aenter__(self) -> "LLMCallTimer":
        self._start = time.monotonic()
        return self

    async def __aexit__(self, *_: Any) -> None:
        self.elapsed_ms = (time.monotonic() - self._start) * 1000
