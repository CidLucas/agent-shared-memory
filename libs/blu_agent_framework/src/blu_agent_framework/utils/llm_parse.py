"""
Utilities for parsing LLM outputs and extracting the first JSON object found.

Functions
- parse_first_json(text: str) -> dict[str, Any] | None

Behavior
1) Try to detect fenced code blocks containing JSON (```json ... ``` or ``` ... ```).
2) Fallback to balanced-brace extraction: find the earliest '{' and scan for a matching '}'.
3) Do small tolerant cleanup (strip common wrappers, remove trailing commas) before json.loads.
4) Return the parsed dict on success, or None on failure.

No external dependencies — uses stdlib only to keep testability portable.
"""
from __future__ import annotations
from typing import Any

import json
import re

FENCED_JSON_RE = re.compile(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", re.IGNORECASE | re.DOTALL)


def _remove_trailing_commas(s: str) -> str:
    # naive removal of trailing commas before closing ] or }
    s = re.sub(r",\s*([}\]])", r"\1", s)
    return s


def _try_load_json(s: str) -> dict[str, Any] | None:
    try:
        return json.loads(s)
    except Exception:
        # try a tolerant cleanup pass
        try:
            cleaned = _remove_trailing_commas(s)
            return json.loads(cleaned)
        except Exception:
            return None


def _extract_balanced_braces(text: str) -> str | None:
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def parse_first_json(text: str) -> dict[str, Any] | None:
    """Parse and return the first JSON object found in `text`.

    Returns parsed dict on success, or None if no valid JSON could be extracted.

    This function intentionally errs on the side of being tolerant — it tries
    fenced code blocks first (common with LLM outputs), then a balanced-brace
    extraction. It performs a small cleanup pass (removing obvious trailing
    commas) before attempting json.loads again.
    """
    if not text:
        return None

    # 1) fenced code block search (```json { ... } ```)
    for m in FENCED_JSON_RE.finditer(text):
        candidate = m.group(1)
        parsed = _try_load_json(candidate)
        if parsed is not None:
            return parsed

    # 2) try to find ANY { ... } balanced block (first occurrence)
    candidate = _extract_balanced_braces(text)
    if candidate:
        parsed = _try_load_json(candidate)
        if parsed is not None:
            return parsed

    # 3) as a last resort, try to find any substring that looks like JSON
    #    by scanning for next '{' and trying expanding windows (limited length)
    for start in range(len(text)):
        if text[start] != "{":
            continue
        # try increasing end positions up to a reasonable limit (100k chars)
        for end in range(start + 1, min(len(text), start + 20000)):
            if text[end] == "}":
                fragment = text[start : end + 1]
                parsed = _try_load_json(fragment)
                if parsed is not None:
                    return parsed

    return None
