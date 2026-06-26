# GOAL: Issue #121 — Aumentar cobertura de testes de 48% para 70%
# BEHAVIOR: B7 — Criar shared_memory_context module
# DECISÃO: extend — módulo faltante do handoff package
# Implementação mínima para teste RED passar (GREEN)
"""Loads context from shared memory for handoff target agents.

For each entity_name in entity_names, calls shared_memory_read (MCP tool
via tool_pool_client.call_tool) and returns
{entity_name: {key: value, ...}}.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def load_shared_memory_context(
    agent_type: str,
    entity_names: list[str],
    tool_pool_client: Any,
) -> dict:
    """Carrega contexto da shared memory para o agente destino.

    Args:
        agent_type: Tipo do agente destino (e.g. "financeiro", "compras").
        entity_names: Lista de entity_names para carregar contexto.
        tool_pool_client: Cliente MCP com método call_tool.

    Returns:
        dict com {entity_name: {key: value, ...}}.
    """
    if not entity_names or tool_pool_client is None:
        return {}

    result: dict[str, dict] = {}

    for entity_name in entity_names:
        try:
            response = await tool_pool_client.call_tool(
                "shared_memory_read",
                {"entity_name": entity_name},
            )
            # MCP response format: {"content": [{"text": "json_string"}]}
            content_list: list = response.get("content", [])
            if content_list:
                raw_text: str = content_list[0].get("text", "{}")
                parsed: dict = json.loads(raw_text)
                key: str = parsed.get("key", "")
                value = parsed.get("value")
                if key:
                    result.setdefault(entity_name, {})[key] = value
        except Exception as exc:
            logger.warning(
                "[shared_memory_context] Failed to load context for entity=%r: %s",
                entity_name,
                exc,
            )

    return result
