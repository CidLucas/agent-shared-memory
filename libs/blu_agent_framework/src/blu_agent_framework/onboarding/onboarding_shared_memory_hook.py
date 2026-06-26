# GOAL: Hook pós-ETL onboarding — Issue #24, Fase 2
# BEHAVIOR: Escreve snapshot inicial da empresa na shared business memory
#           após conclusão do ETL onboarding.
# DECISÃO: create_new — módulo independente dentro de blu_agent_framework/onboarding/
"""
onboarding_shared_memory_hook.py — Hook pós-ETL onboarding.

Após a conclusão do ETL onboarding (geração do structured_context via
onboarding_context_build skill), este hook escreve o snapshot inicial da
empresa na shared business memory.

O que o hook faz:
  1. Escreve 3 entradas no entity_type 'client':
       - company_profile → company_profile dict
       - brand_voice     → brand_voice dict
       - goals           → goals list
  2. Escreve 1 entrada no entity_type 'snapshot':
       - entity_name: 'onboarding:{company_name_slug}'
       - key: 'initial'
       - value: structured_context completo
       - metadata (frontmatter): {tipo, dimensao, periodo, gerado_em, ...}
  3. Escreve 1 entrada na shared_business_memory_meta com metadados do hook.

Source: 'system' (tem permissão total de escrita em shared_business_memory).
Versão: 1 (onboarding_version).

Design: docs/llm_wiki/SHARED_MEMORY_DESIGN.md
Issue:  #24 — Fase 2, Hook pós-ETL onboarding.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SBM_TABLE = "shared_business_memory"
_SBM_META_TABLE = "shared_business_memory_meta"

# Metadata comum a todas as entradas escritas por este hook
_ONBOARDING_METADATA: dict[str, Any] = {
    "onboarding_version": 1,
    "generated_by": "onboarding_complete_routine",
}

# Frontmatter do snapshot (vai na coluna metadata da linha 'snapshot')
_SNAPSHOT_FRONTMATTER_TEMPLATE: dict[str, Any] = {
    "tipo": "snapshot",
    "dimensao": "clientes",
    "periodo": "inicial",
    "gerado_em": None,       # preenchido em runtime
    "gerado_por": "system",
    "versao": 1,
    "template_version": 1,
    "fontes": ["onboarding_wizard", "website_scrape"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_snake_case(name: str) -> str:
    """Convert a company name to snake_case for use as entity_name.

    Examples:
        "Acme Corp"       → "acme_corp"
        "João's Café!"    → "joao_cafe"
        "  Tech 4 Good  " → "tech_4_good"
    """
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9\s]", "", s)  # remove non-alphanumeric (except space)
    s = re.sub(r"\s+", "_", s)          # spaces → underscore
    return s.strip("_")


def _now_iso() -> str:
    """Return current UTC datetime as ISO-8601 string."""
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def write_onboarding_snapshot_to_shared_memory(
    client_id: str,
    company_name: str,
    structured_context: dict[str, Any],
) -> dict[str, Any]:
    """Write the initial onboarding snapshot to shared business memory.

    Call this function after the ETL onboarding generates ``structured_context``
    (via the ``onboarding_context_build`` skill).  It performs three categories
    of writes:

    **Client entries** (entity_type='client')
        Three facts are written — ``company_profile``, ``brand_voice``, and
        ``goals`` — each as a separate row keyed by ``{entity_name}/{key}``.

    **Snapshot entry** (entity_type='snapshot')
        A single row with entity_name ``onboarding:{slug}``, key ``initial``,
        value = the entire ``structured_context`` dict, and a frontmatter
        block in the metadata column describing provenance.

    **Meta entry** (shared_business_memory_meta)
        A synthesis_output row recording metadata about this hook execution.

    Errors are NEVER raised to the caller — they are logged and collected in
    the returned result dict so the hook never breaks the onboarding flow.

    Args:
        client_id: UUID string of the client (tenant).
        company_name: Company name (used to derive entity_name in snake_case).
        structured_context: The full structured_context dict produced by the
            onboarding_context_build skill.  Expected to contain at least
            the keys ``company_profile``, ``brand_voice``, and ``goals``.

    Returns:
        A summary dict with the following structure::

            {
                "client_entries": [
                    {"key": "company_profile", "id": int | None, "success": bool, ...},
                    ...
                ],
                "snapshot_entry": {
                    "entity_name": str, "id": int | None, "success": bool, ...
                },
                "meta_entry": {
                    "entity_name": str, "id": int | None, "success": bool, ...
                },
                "errors": [str, ...],
            }

        The ``errors`` list is empty on full success.  Each per-entry dict
        includes an ``error`` key when its ``success`` is False.
    """
    result: dict[str, Any] = {
        "client_entries": [],
        "snapshot_entry": None,
        "meta_entry": None,
        "errors": [],
    }

    entity_name = _to_snake_case(company_name)
    if not entity_name:
        msg = f"Company name '{company_name}' produced empty entity_name"
        logger.error(msg)
        result["errors"].append(msg)
        return result

    now_iso = _now_iso()

    # ------------------------------------------------------------------
    # 0. Obtain Supabase client (service role = bypasses RLS)
    # ------------------------------------------------------------------
    try:
        from blu_supabase_client import get_supabase_client

        db = get_supabase_client(use_service_role=True)
    except Exception as exc:
        msg = f"Failed to obtain Supabase client: {exc}"
        logger.exception(msg)
        result["errors"].append(msg)
        return result

    # ------------------------------------------------------------------
    # 1. Client entries — company_profile, brand_voice, goals
    # ------------------------------------------------------------------
    client_schema_key = {
        "company_profile": structured_context.get("company_profile", {}),
        "brand_voice": structured_context.get("brand_voice", {}),
        "goals": structured_context.get("goals", []),
    }

    for key, value in client_schema_key.items():
        entry_result: dict[str, Any] = {"key": key, "success": False}
        try:
            payload = {
                "client_id": client_id,
                "entity_type": "client",
                "entity_name": entity_name,
                "key": key,
                "value": value,
                "category": "context",
                "source": "system",
                "confidence": 1.0,
                "metadata": _ONBOARDING_METADATA,
                "version": 1,
            }
            resp = (
                db.schema("public")
                .table(_SBM_TABLE)
                .upsert(
                    payload,
                    on_conflict="client_id,entity_type,entity_name,key",
                    default_to_null=False,
                )
                .execute()
            )
            row = resp.data[0] if resp.data else None
            entry_result["id"] = row.get("id") if row else None
            entry_result["success"] = row is not None
            if not row:
                entry_result["error"] = "No data returned from upsert"
        except Exception as exc:
            msg = f"Failed to write client entry '{key}': {exc}"
            logger.exception(msg)
            entry_result["error"] = str(exc)
            result["errors"].append(msg)

        result["client_entries"].append(entry_result)

    # ------------------------------------------------------------------
    # 2. Snapshot entry — complete structured_context
    # ------------------------------------------------------------------
    snapshot_entity_name = f"onboarding:{entity_name}"
    snapshot_frontmatter = dict(_SNAPSHOT_FRONTMATTER_TEMPLATE)
    snapshot_frontmatter["gerado_em"] = now_iso

    snapshot_entry: dict[str, Any] = {
        "entity_name": snapshot_entity_name,
        "success": False,
    }
    try:
        payload = {
            "client_id": client_id,
            "entity_type": "snapshot",
            "entity_name": snapshot_entity_name,
            "key": "initial",
            "value": structured_context,
            "category": "context",
            "source": "system",
            "confidence": 1.0,
            "metadata": snapshot_frontmatter,
            "version": 1,
        }
        resp = (
            db.schema("public")
            .table(_SBM_TABLE)
            .upsert(
                payload,
                on_conflict="client_id,entity_type,entity_name,key",
                default_to_null=False,
            )
            .execute()
        )
        row = resp.data[0] if resp.data else None
        snapshot_entry["id"] = row.get("id") if row else None
        snapshot_entry["success"] = row is not None
        if not row:
            snapshot_entry["error"] = "No data returned from upsert"
    except Exception as exc:
        msg = f"Failed to write snapshot entry: {exc}"
        logger.exception(msg)
        snapshot_entry["error"] = str(exc)
        result["errors"].append(msg)

    result["snapshot_entry"] = snapshot_entry

    # ------------------------------------------------------------------
    # 3. Meta entry — hook execution metadata
    # ------------------------------------------------------------------
    meta_body: dict[str, Any] = {
        "hook": "onboarding_shared_memory_hook",
        "version": 1,
        "company_name": company_name,
        "entity_name": entity_name,
        "snapshot_entity_name": snapshot_entity_name,
        "structured_context_keys": list(structured_context.keys()),
        "written_at": now_iso,
        "company_profile_size": len(
            json.dumps(structured_context.get("company_profile", {}))
        ),
        "brand_voice_size": len(
            json.dumps(structured_context.get("brand_voice", {}))
        ),
        "goals_count": len(structured_context.get("goals", [])),
    }

    meta_entry: dict[str, Any] = {
        "entity_name": entity_name,
        "success": False,
    }
    try:
        meta_payload = {
            "client_id": client_id,
            "entity_type": "synthesis_output",
            "entity_name": entity_name,
            "key": "onboarding_snapshot",
            "body": meta_body,
            "source": "system",
            "confidence": 1.0,
        }
        resp = (
            db.schema("public")
            .table(_SBM_META_TABLE)
            .upsert(
                meta_payload,
                on_conflict="client_id,entity_type,entity_name,key",
                default_to_null=False,
            )
            .execute()
        )
        row = resp.data[0] if resp.data else None
        meta_entry["id"] = row.get("id") if row else None
        meta_entry["success"] = row is not None
        if not row:
            meta_entry["error"] = "No data returned from upsert"
    except Exception as exc:
        msg = f"Failed to write meta entry: {exc}"
        logger.exception(msg)
        meta_entry["error"] = str(exc)
        result["errors"].append(msg)

    result["meta_entry"] = meta_entry

    # ------------------------------------------------------------------
    # Summary log
    # ------------------------------------------------------------------
    client_successes = sum(1 for e in result["client_entries"] if e["success"])
    logger.info(
        "onboarding_shared_memory_hook | client=%s (%s) | "
        "client_entries=%d/%d ok | snapshot=%s | meta=%s | errors=%d",
        client_id,
        entity_name,
        client_successes,
        len(result["client_entries"]),
        snapshot_entry.get("success", False),
        meta_entry.get("success", False),
        len(result["errors"]),
    )

    return result
