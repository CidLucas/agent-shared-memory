"""memory_pre_flight.py — Pre-flight Shared Memory Context (T1.1)

Reads recent agent execution context from shared_business_memory BEFORE
the agent runs, injecting it into AgentState.agent_preflight_context.

Design decisions (from DD-PF-*):
  - DD-PF-01: Dedicated module, parallel to memory_post_flight.py
  - DD-PF-03: Reads agent_metadata (all) + agent_result (decision/finding/summary)
  - DD-PF-04: Last 5 executions per agent_slug (configurable via MAX_PREFLIGHT_EXECUTIONS)
  - DD-PF-06: Internal tool (not MCP-exposed) — import the logic function directly
  - DD-PF-07: Fail-open — any error logs warning and returns empty dict
"""

import logging
import os

from fastmcp import FastMCP

from tool_pool_api.server.tool_modules import register_module

logger = logging.getLogger(__name__)

_MAX_EXECUTIONS_DEFAULT = 5
_AGENT_RESULT_KEY_PREFIXES = ("decision:", "finding:", "summary:execution")
_TABLE = "shared_business_memory"


def _max_executions() -> int:
    """Resolve max_executions from env, clamped to [1, 50]."""
    try:
        val = int(os.getenv("MAX_PREFLIGHT_EXECUTIONS", str(_MAX_EXECUTIONS_DEFAULT)))
        return max(1, min(val, 50))
    except (TypeError, ValueError):
        return _MAX_EXECUTIONS_DEFAULT


async def _shared_memory_pre_flight_logic(
    client_id: str,
    agent_slug: str,
    max_executions: int | None = None,
) -> dict:
    """Read recent agent execution context from shared_business_memory.

    Args:
        client_id: The client UUID isolating the tenant.
        agent_slug: The agent slug (e.g. "frontdesk", "crm_specialist").
        max_executions: Override for MAX_PREFLIGHT_EXECUTIONS (None = use env).

    Returns:
        {
            "agent_metadata": [...],     # last N agent_metadata rows
            "agent_results": [...],       # decision/finding/summary rows
            "execution_count": int,
            "agent_slug": str,
        }
    """
    limit = max_executions if max_executions is not None else _max_executions()

    try:
        # Lazy import — avoids circular dependency at registration time
        from blu_supabase_client import get_supabase_client

        db = await get_supabase_client()

        # 1. Read agent_metadata: last N executions ordered by updated_at DESC
        metadata_result = await (
            db.schema("public")
            .table(_TABLE)
            .select("*")
            .eq("client_id", client_id)
            .eq("entity_type", "agent_metadata")
            .eq("entity_name", agent_slug)
            .order("updated_at", desc=True)
            .limit(limit)
            .execute()
        )

        # 2. Read agent_result with preferred key prefixes
        #    Since Supabase .like() only supports a single pattern, we use .in_()
        #    with an OR-filter approach via multiple requests.
        agent_results: list[dict] = []

        for prefix in _AGENT_RESULT_KEY_PREFIXES:
            prefix_results = await (
                db.schema("public")
                .table(_TABLE)
                .select("*")
                .eq("client_id", client_id)
                .eq("entity_type", "agent_result")
                .eq("entity_name", agent_slug)
                .like("key", f"{prefix}%")
                .order("updated_at", desc=True)
                .limit(limit * 3)  # multiple keys per execution possible
                .execute()
            )
            if prefix_results.data:
                agent_results.extend(prefix_results.data)

        # Deduplicate by id (same row could match multiple prefixes)
        seen: set[str] = set()
        deduped_results: list[dict] = []
        for row in agent_results:
            rid = row.get("id")
            if rid and rid not in seen:
                seen.add(rid)
                deduped_results.append(row)

        # Sort by updated_at desc
        deduped_results.sort(key=lambda r: r.get("updated_at", ""), reverse=True)

        execution_count = len(metadata_result.data)

        return {
            "agent_metadata": metadata_result.data,
            "agent_results": deduped_results,
            "execution_count": execution_count,
            "agent_slug": agent_slug,
        }

    except Exception:
        logger.warning(
            f"Pre-flight failed for agent_slug={agent_slug}, client_id={client_id} — "
            f"returning empty context (fail-open, DD-PF-07)",
            exc_info=True,
        )
        return {
            "agent_metadata": [],
            "agent_results": [],
            "execution_count": 0,
            "agent_slug": agent_slug,
        }


# ---------------------------------------------------------------------------
# MCP Tool Registration (internal — not exposed to agents via MCP)
# ---------------------------------------------------------------------------


@register_module
def register_tools(mcp: FastMCP) -> list[str]:
    """Register shared_memory_pre_flight as an internal tool.

    This tool is intended for programmatic use by ChatService (service.py),
    not for direct agent consumption via MCP.  The logic function
    ``_shared_memory_pre_flight_logic`` should be imported directly.
    """
    registered: list[str] = []

    @mcp.tool(
        name="shared_memory_pre_flight",
        description=(
            "INTERNAL: Read recent agent execution context from shared_business_memory. "
            "Returns agent_metadata and agent_results for the given agent_slug. "
            "Used by ChatService to inject pre-flight context into AgentState."
        ),
    )
    async def shared_memory_pre_flight(
        client_id: str,
        agent_slug: str,
        max_executions: int = _MAX_EXECUTIONS_DEFAULT,
    ) -> dict:
        return await _shared_memory_pre_flight_logic(
            client_id, agent_slug, max_executions
        )

    registered.append("shared_memory_pre_flight")
    return registered
