"""
memory_post_flight.py — Post-flight persistence for agent results (T1.2).

Design doc: docs/llm_wiki/SHARED_MEMORY_DESIGN.md (Fase 1 / T1.2)

Internal module (DD-06): not exposed via MCP. Called by the post-flight hook
in ChatService (service.py) via fire-and-forget after each agent execution.

Entity types (DD-02):
  - agent_result      — what the agent found/decided (finding:, decision:, summary:)
  - agent_metadata    — execution metadata (session_id, elapsed, tool_usage:*)
  - agent_link_pending — suggested links saved to shared_memory_links with
                          source='agent_pending' for later validation (T4.4)

Naming convention (DD-03):
  decision:   — agent decision (e.g. decision:priorizar_fornecedor_x)
  finding:    — extracted insight (e.g. finding:cliente_atrasado_3_meses)
  summary:    — execution summary
  tool_usage: — tool name used (e.g. tool_usage:execute_sql)

Noise suppression (DD-04): upsert by unique key means only the last
significant state per (client_id, entity_type, entity_name, key) is persisted.
"""

import json
import logging

from blu_supabase_client import get_supabase_client

from tool_pool_api.server.tool_modules import register_module
from tool_pool_api.server.utils.entity import normalize_entity_name

logger = logging.getLogger(__name__)

_TABLE = "shared_business_memory"
_LINKS_TABLE = "shared_memory_links"

_VALID_PREFIXES: frozenset[str] = frozenset({"decision:", "finding:", "summary:", "tool_usage:"})

# Maximum length for agent_result summary text stored in value
_MAX_SUMMARY_CHARS = 2000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_key_prefix(key: str) -> None:
    """Warn if key doesn't follow the naming convention (non-fatal)."""
    for prefix in _VALID_PREFIXES:
        if key.startswith(prefix):
            return
    logger.warning(
        "[PostFlight] Key '%s' does not start with a recognized prefix: %s",
        key,
        sorted(_VALID_PREFIXES),
    )


# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------


async def _shared_memory_post_flight_logic(
    client_id: str,
    agent_slug: str,
    session_id: str,
    agent_result: dict | None = None,
    agent_metadata: dict | None = None,
    suggested_links: list[dict] | None = None,
) -> dict:
    """Persist agent results, metadata, and suggested links in shared memory.

    Called fire-and-forget by the ChatService hook. Never blocks the user.

    Args:
        client_id:   Client UUID (from context).
        agent_slug:  Agent slug (e.g. 'crm', 'frontdesk').
        session_id:  Session UUID for this agent run.
        agent_result: {
            "summary": str,       # truncated to _MAX_SUMMARY_CHARS
            "tool_calls": [str],  # list of tool names used
        }
        agent_metadata: {
            "session_id": str,
            "agent_slug": str,
            "elapsed_seconds": float,
        }
        suggested_links: [
            {
                "source_entity_type": str,
                "source_entity_name": str,
                "target_entity_type": str,
                "target_entity_name": str,
                "link_type": str,
            },
            ...
        ]

    Returns:
        dict with summary of what was persisted:
            agent_result_entries: int
            agent_metadata_entries: int
            links_created: int
    """
    db = await get_supabase_client()
    result_entries = 0
    metadata_entries = 0
    links_created = 0

    session_short = session_id[:8] if session_id else ""
    result_entity_name = f"{agent_slug}:{session_short}"

    # ------------------------------------------------------------------
    # 1. agent_result — summary + findings/decisions + tool usage
    # ------------------------------------------------------------------
    if agent_result and isinstance(agent_result, dict):
        summary_text = str(agent_result.get("summary", "") or "")
        tool_calls = agent_result.get("tool_calls") or []

        if summary_text.strip():
            payload = {
                "client_id": client_id,
                "entity_type": "agent_result",
                "entity_name": normalize_entity_name(result_entity_name),
                "key": "summary:execution",
                "value": json.dumps(
                    {"text": summary_text[:_MAX_SUMMARY_CHARS]}
                ),
                "category": "context",
                "source": "specialist",
                "confidence": 1.0,
                "metadata": json.dumps({"agent_slug": agent_slug}),
            }
            try:
                await (
                    db.schema("public")
                    .table(_TABLE)
                    .upsert(
                        payload,
                        on_conflict="client_id,entity_type,entity_name,key",
                    )
                    .execute()
                )
                result_entries += 1
            except Exception as exc:
                logger.warning(
                    "[PostFlight] Failed to upsert summary for agent=%s: %s",
                    agent_slug,
                    exc,
                )

        # Tool usage entries (DD-03: tool_usage:<tool_name>) — B3.2 batch upsert
        if tool_calls:
            try:
                await (
                    db.schema("public")
                    .table(_TABLE)
                    .upsert(
                        [
                            {
                                "client_id": client_id,
                                "entity_type": "agent_result",
                                "entity_name": normalize_entity_name(result_entity_name),
                                "key": f"tool_usage:{tool_name_str}",
                                "value": json.dumps({"tool": tool_name_str}),
                                "category": "context",
                                "source": "specialist",
                                "confidence": 1.0,
                                "metadata": json.dumps({}),
                            }
                            for tn in tool_calls
                            if (tool_name_str := str(tn).strip())
                        ],
                        on_conflict="client_id,entity_type,entity_name,key",
                    )
                    .execute()
                )
                result_entries += sum(1 for tn in tool_calls if str(tn).strip())
            except Exception as exc:
                logger.warning(
                    "[PostFlight] Failed to batch upsert tool_usage: %s",
                    exc,
                )

    # ------------------------------------------------------------------
    # 2. agent_metadata — session_id, elapsed, agent_slug
    # ------------------------------------------------------------------
    metadata_entity_name = normalize_entity_name(agent_slug)

    if agent_metadata and isinstance(agent_metadata, dict):
        meta_fields = {
            "session_id": session_id,
            "elapsed_seconds": agent_metadata.get("elapsed_seconds", 0),
            "agent_slug": agent_slug,
        }

        try:
            await (
                db.schema("public")
                .table(_TABLE)
                .upsert(
                    [
                        {
                            "client_id": client_id,
                            "entity_type": "agent_metadata",
                            "entity_name": metadata_entity_name,
                            "key": k,
                            "value": json.dumps(meta_fields[k]),
                            "category": "context",
                            "source": "system",
                            "confidence": 1.0,
                            "metadata": json.dumps({}),
                        }
                        for k in meta_fields
                    ],
                    on_conflict="client_id,entity_type,entity_name,key",
                )
                .execute()
            )
            metadata_entries = len(meta_fields)
        except Exception as exc:
            logger.warning(
                "[PostFlight] Failed to batch upsert metadata for agent=%s: %s",
                agent_slug,
                exc,
            )

    # ------------------------------------------------------------------
    # 3. agent_link_pending — suggested semantic links (DD-04: DQ4) — B3.2 batch insert
    # ------------------------------------------------------------------
    if suggested_links and isinstance(suggested_links, list):
        link_payloads: list[dict] = []
        for sl in suggested_links:
            if not isinstance(sl, dict):
                continue
            try:
                source_et = str(sl.get("source_entity_type", "")).strip()
                source_en = normalize_entity_name(
                    str(sl.get("source_entity_name", ""))
                )
                target_et = str(sl.get("target_entity_type", "")).strip()
                target_en = normalize_entity_name(
                    str(sl.get("target_entity_name", ""))
                )
                link_type = str(sl.get("link_type", "")).strip().lower()

                if not all([source_et, source_en, target_et, target_en, link_type]):
                    logger.warning(
                        "[PostFlight] Skipping incomplete link: %s", sl
                    )
                    continue

                link_payloads.append({
                    "client_id": client_id,
                    "source_entity_type": source_et,
                    "source_entity_name": source_en,
                    "target_entity_type": target_et,
                    "target_entity_name": target_en,
                    "link_type": link_type,
                    "source": "agent_pending",
                    "confidence": 0.5,
                    "metadata": json.dumps(
                        {
                            "agent_slug": agent_slug,
                            "session_id": session_id,
                        }
                    ),
                })
            except Exception as exc:
                logger.warning(
                    "[PostFlight] Failed to build link payload %s: %s",
                    sl,
                    exc,
                )

        if link_payloads:
            try:
                await (
                    db.schema("public")
                    .table(_LINKS_TABLE)
                    .insert(link_payloads)
                    .execute()
                )
                links_created = len(link_payloads)
            except Exception as exc:
                err_str = str(exc).lower()
                if "duplicate key" in err_str or "uq_shared_memory_link" in err_str:
                    logger.debug(
                        "[PostFlight] Duplicate links skipped in batch of %d",
                        len(link_payloads),
                    )
                else:
                    logger.warning(
                        "[PostFlight] Failed to batch insert links: %s",
                        exc,
                    )

    summary = {
        "agent_result_entries": result_entries,
        "agent_metadata_entries": metadata_entries,
        "links_created": links_created,
    }
    logger.info(
        "[PostFlight] agent=%s session=%s → %s",
        agent_slug,
        session_short,
        summary,
    )
    return summary


# ---------------------------------------------------------------------------
# Module registration (DD-06: internal tool — no MCP exposure)
# ---------------------------------------------------------------------------


@register_module
def register_tools(mcp) -> list[str]:
    """Register post-flight module. Returns empty list (internal only)."""
    logger.info("[PostFlight Module] Internal tool ready (not exposed via MCP).")
    return []
