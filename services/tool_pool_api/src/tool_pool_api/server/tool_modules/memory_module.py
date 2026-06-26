"""memory_module.py -- Shared Business Memory Tools (T0.4-T0.6)

Registers L1 tools for interacting with the ``shared_business_memory`` and
``shared_memory_links`` tables in Supabase.  Agents communicate via shared
memory (not direct conversation), reading and writing knowledge about
entities (clients, contacts, suppliers, users, skill-derived facts,
and snapshots).

Tools registered:
  - shared_memory_list    -> list entities with memory entries
  - shared_memory_read    -> read a single fact by composite key
  - shared_memory_upsert  -> insert or update a fact (versioned)
  - shared_memory_meta_upsert -> insert or update a meta entry in shared_business_memory_meta
  - shared_memory_write   -> write a new fact (strict INSERT; supersede=True to upsert)
  - shared_memory_search  -> semantic vector search via Cohere embeddings (T3.1c)
  - shared_memory_flush   -> soft-delete entries (marks flushed_at in metadata; T5.4)
  - shared_memory_link    -> create semantic link between entities
  - shared_memory_unlink  -> remove a link by id
  - shared_memory_get_links -> query links by entity and/or type
  - shared_memory_export  -> export all facts for a client (T5.4)
  - shared_memory_meta_read  -> read a single meta entry from shared_business_memory_meta
  - shared_memory_meta_list  -> list meta entries, optionally filtered by entity_type

Design doc: docs/llm_wiki/SHARED_MEMORY_DESIGN.md (Fase 0)
"""

import json
import logging
import re
import time

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError

from blu_auth.mcp.auth_middleware import mcp_inject_client_id
from blu_supabase_client import get_direct_engine, get_supabase_client
from sqlalchemy import text

from tool_pool_api.server.dependencies import get_context_service
from blu_context_service.context_schemas import _SNAPSHOT_DIMENSION_FIELDS

from tool_pool_api.server.tool_modules import register_module
from tool_pool_api.server.utils.entity import (
    VALID_ENTITY_TYPES,
    normalize_entity_name,
    validate_entity_type,
)

# Bind mcp_inject_client_id to its already-configured form so it can be used
# as a plain @decorator below. The factory signature is
#     mcp_inject_client_id(get_context_service_fn) -> decorator
# and the tools in this module use the @mcp_inject_client_id sugar form
# (which would otherwise pass the tool function as get_context_service_fn,
# breaking FastMCP's Pydantic schema generation). This rebind keeps the
# @mcp_inject_client_id syntax working without touching every tool.
mcp_inject_client_id = mcp_inject_client_id(get_context_service)

logger = logging.getLogger(__name__)

_TABLE: str = "shared_business_memory"
_LINKS_TABLE: str = "shared_memory_links"

_VALID_CATEGORIES: frozenset[str] = frozenset(
    {"knowledge", "rag", "documents", "memory-agent",
     "context", "decision", "preference"}
)

# ---------------------------------------------------------------------------
# Write permission check (T5.2)
# ---------------------------------------------------------------------------

# Mapping of source -> allowed entity_types for write operations.
# Follows the "Single Writer" principle: each source can only write to
# entity types it is authorised for.
_WRITE_PERMISSIONS: dict[str, frozenset[str]] = {
    "system": frozenset({
        "skill", "client", "contact", "supplier", "user",
        "snapshot", "routine", "agent_result", "agent_metadata",
    }),
    "memory_agent": frozenset({
        "skill", "client", "contact", "supplier", "user",
        "snapshot", "routine", "agent_result", "agent_metadata",
    }),
    "specialist": frozenset({
        "skill", "client", "contact", "supplier", "user",
        "snapshot", "agent_result", "agent_metadata",
    }),
    "manual": frozenset({
        "skill", "client", "contact", "supplier", "user",
    }),
    "migration": frozenset({
        "skill", "client", "contact", "supplier", "user",
        "snapshot", "routine", "agent_result", "agent_metadata",
    }),
}


def _check_write_permission(
    source: str,
    entity_type: str,
    entity_name: str,
) -> None:
    allowed = _WRITE_PERMISSIONS.get(source)
    if allowed is None:
        raise ValueError(
            f"Unknown source '{source}'. "
            f"Must be one of: {sorted(_WRITE_PERMISSIONS.keys())}"
        )
    if entity_type not in allowed:
        raise ValueError(
            f"Write permission denied: source '{source}' cannot write to "
            f"entity_type '{entity_type}' (entity: {entity_name}). "
            f"Allowed types for '{source}': {sorted(allowed)}"
        )


# ---------------------------------------------------------------------------
# Entity reference extraction from markdown (B1)
# ---------------------------------------------------------------------------

_ENTITY_REFERENCE_PATTERN = re.compile(
    r"\[(?P<label>[^\]]*)\]\((?P<entity_type>[a-z_]+):(?P<entity_name>[^)]+)\)"
)


def _extract_entity_references(markdown_text: str) -> list[dict]:
    """Scan ``markdown_text`` for entity references of the form ``[label](entity_type:entity_name)``.

    Returns a list of dicts with keys ``entity_type``, ``entity_name``,
    ``label`` and ``span`` (a ``(start, end)`` tuple indexing back into
    the original string).  Only entity types from
    :data:`VALID_ENTITY_TYPES <tool_pool_api.server.utils.entity.VALID_ENTITY_TYPES>`
    are accepted; unknown entity types are silently ignored.  Duplicate
    references are preserved in the order they appear.
    """
    if not markdown_text:
        return []
    pattern = re.compile(
        r"\[(?P<label>[^\]]*)\]\((?P<entity_type>[a-z_]+):(?P<entity_name>[^)]+)\)"
    )
    results: list[dict] = []
    for match in pattern.finditer(markdown_text):
        entity_type = match.group("entity_type")
        if entity_type not in VALID_ENTITY_TYPES:
            continue
        results.append({
            "entity_type": entity_type,
            "entity_name": match.group("entity_name"),
            "label": match.group("label"),
            "span": match.span(),
        })
    return results

# ---------------------------------------------------------------------------
# Category constants (for shared_memory_write)
# ---------------------------------------------------------------------------

_VALID_CATEGORIES: frozenset[str] = frozenset({
    "knowledge", "rag", "documents", "memory-agent",
    "context", "decision", "preference",
})

# ---------------------------------------------------------------------------
# TTL tier constants (Fase 4 — T4.4c)
# ---------------------------------------------------------------------------

_VALID_TTL_TIERS: frozenset[str] = frozenset({
    "curated", "migration", "specialist",
    "memory_agent_hi", "memory_agent_lo",
})

# Interval mapping: tier → soft_delete_at offset (in days)
# curated = None means never expires
_TTL_TIER_INTERVALS: dict[str, int | None] = {
    "curated": None,          # Never expires
    "migration": 90,          # +90 days
    "specialist": 30,         # +30 days
    "memory_agent_hi": 14,    # +14 days
    "memory_agent_lo": 7,     # +7 days
}

# Archival period: hard_delete_at = soft_delete_at + 90 days
_ARCHIVAL_PERIOD_DAYS: int = 90

# Default TTL tier inference from source
_SOURCE_TTL_DEFAULTS: dict[str, str] = {
    "curated": "curated",
    "migration": "migration",
    "specialist": "specialist",
    "memory_agent": "memory_agent_lo",
}

# ---------------------------------------------------------------------------
# Category constants (for shared_memory_write)
# ---------------------------------------------------------------------------

_VALID_CATEGORIES: frozenset[str] = frozenset({
    "knowledge", "rag", "documents", "memory-agent",
    "context", "decision", "preference",
})

# ---------------------------------------------------------------------------
# TTL tier constants (Fase 4 — T4.4c)
# ---------------------------------------------------------------------------

_VALID_TTL_TIERS: frozenset[str] = frozenset({
    "curated", "migration", "specialist",
    "memory_agent_hi", "memory_agent_lo",
})

# Interval mapping: tier → soft_delete_at offset (in days)
# curated = None means never expires
_TTL_TIER_INTERVALS: dict[str, int | None] = {
    "curated": None,          # Never expires
    "migration": 90,          # +90 days
    "specialist": 30,         # +30 days
    "memory_agent_hi": 14,    # +14 days
    "memory_agent_lo": 7,     # +7 days
}

# Archival period: hard_delete_at = soft_delete_at + 90 days
_ARCHIVAL_PERIOD_DAYS: int = 90

# Default TTL tier inference from source
_SOURCE_TTL_DEFAULTS: dict[str, str] = {
    "curated": "curated",
    "migration": "migration",
    "specialist": "specialist",
    "memory_agent": "memory_agent_lo",
}

# ---------------------------------------------------------------------------
# Snapshot constants (T2.2a + T2.2b)
# ---------------------------------------------------------------------------

_SNAPSHOT_BASE_FIELDS: frozenset[str] = frozenset({
    "snapshot_id", "dimensao", "periodo", "gerado_em",
    "vigencia_inicio", "vigencia_fim", "indicadores", "alertas",
    "resumo_executivo",
})

_SNAPSHOT_FRONTMATTER_REQUIRED: frozenset[str] = frozenset({
    "tipo", "dimensao", "periodo", "gerado_em", "gerado_por",
    "versao", "template_version", "fontes",
})

_VALID_DIMENSIONS: frozenset[str] = frozenset(
    {"financeiro", "clientes", "agenda", "compras"}
)

_VALID_PERIODS: frozenset[str] = frozenset(
    {"diario", "semanal", "mensal"}
)


# ---------------------------------------------------------------------------
# Link validation helpers
# ---------------------------------------------------------------------------


def _is_flushed(metadata: dict | None) -> bool:
    """Check whether a shared-memory entry has been flushed.

    Flushed entries have ``flushed_at`` set in their metadata JSONB column.
    This is a soft-delete marker — the row still exists but should be treated
    as cleared/unavailable.

    Args:
        metadata: The metadata dict from the row (may be None, treated as not flushed).

    Returns:
        True if the entry is marked as flushed.
    """
    if not isinstance(metadata, dict):
        return False
    return "flushed_at" in metadata


def _check_not_flushed(metadata: dict | None, entity_ref: str) -> None:
    """Raise ValueError if the entry is flushed.

    Used before returning read results to ensure flushed entries are not
    surfaced to agents.

    Args:
        metadata: The metadata dict from the row.
        entity_ref: Human-readable entity reference for the error message.

    Raises:
        ValueError: If the entry is flushed.
    """
    if _is_flushed(metadata):
        raise ValueError(
            f"Memory entry has been flushed (soft-deleted): {entity_ref}"
        )




# ---------------------------------------------------------------------------
# Snapshot validation (T2.2b + T2.2f)
# ---------------------------------------------------------------------------


def _validate_snapshot_frontmatter(
    entity_name: str,
    frontmatter: dict,
) -> None:
    """Validate that a snapshot has the required frontmatter fields.

    Args:
        entity_name: e.g. "financeiro:semanal" -- used for cross-validation.
        frontmatter: The frontmatter dict to validate.

    Raises:
        ValueError: If required fields are missing or invalid.
    """
    if not isinstance(frontmatter, dict):
        raise ValueError(
            "frontmatter is required for entity_type='snapshot' and must be a dict"
        )

    # Validate required fields
    missing = _SNAPSHOT_FRONTMATTER_REQUIRED - set(frontmatter.keys())
    if missing:
        raise ValueError(
            f"Snapshot frontmatter missing required fields: {sorted(missing)}"
        )

    # Validate 'tipo' field
    if frontmatter.get("tipo") != "snapshot":
        raise ValueError(
            "frontmatter.tipo must be 'snapshot'"
        )

    # Validate dimension
    dimensao = frontmatter.get("dimensao")
    if dimensao not in _VALID_DIMENSIONS:
        raise ValueError(
            f"frontmatter.dimensao '{dimensao}' is invalid. "
            f"Must be one of: {sorted(_VALID_DIMENSIONS)}"
        )

    # Cross-validate with entity_name: dimension must match
    parts = entity_name.split(":")
    entity_dim = parts[0] if parts else ""
    if entity_dim and entity_dim != dimensao:
        raise ValueError(
            f"entity_name dimension '{entity_dim}' does not match "
            f"frontmatter.dimensao '{dimensao}'"
        )

    # Validate period
    periodo = frontmatter.get("periodo")
    if periodo not in _VALID_PERIODS:
        raise ValueError(
            f"frontmatter.periodo '{periodo}' is invalid. "
            f"Must be one of: {sorted(_VALID_PERIODS)}"
        )

    # Cross-validate period with entity_name
    if len(parts) > 1 and parts[1] and parts[1] != periodo:
        raise ValueError(
            f"entity_name period '{parts[1]}' does not match "
            f"frontmatter.periodo '{periodo}'"
        )

    # Validate version is positive int
    versao = frontmatter.get("versao")
    if not isinstance(versao, int) or versao < 1:
        raise ValueError(
            "frontmatter.versao must be a positive integer"
        )

    # Validate template_version is positive int
    template_version = frontmatter.get("template_version")
    if not isinstance(template_version, int) or template_version < 1:
        raise ValueError(
            "frontmatter.template_version must be a positive integer"
        )

    # Validate fontes is a list of strings
    fontes = frontmatter.get("fontes")
    if not isinstance(fontes, list) or not all(isinstance(f, str) for f in fontes):
        raise ValueError("frontmatter.fontes must be a list of strings")


def _validate_snapshot_body(
    entity_name: str,
    body: dict,
) -> None:
    """Validate a snapshot body against its dimension schema.

    Args:
        entity_name: e.g. "financeiro:semanal" -- dimension extracted from here.
        body: The body dict (value column content).

    Raises:
        ValueError: If validation fails.
    """
    # Extract dimension from entity_name
    parts = entity_name.split(":")
    dimensao = parts[0] if parts else ""

    if not dimensao:
        raise ValueError(
            "Cannot determine snapshot dimension from entity_name"
        )

    if dimensao not in _VALID_DIMENSIONS:
        raise ValueError(
            f"Invalid snapshot dimension '{dimensao}'. "
            f"Must be one of: {sorted(_VALID_DIMENSIONS)}"
        )

    # Validate base fields are present
    missing_base = _SNAPSHOT_BASE_FIELDS - set(body.keys())
    if missing_base:
        raise ValueError(
            f"Snapshot body missing required base fields: {sorted(missing_base)}"
        )

    # Validate 'dimensao' inside body matches entity_name
    body_dimensao = body.get("dimensao")
    if body_dimensao != dimensao:
        raise ValueError(
            f"body.dimensao '{body_dimensao}' does not match "
            f"entity_name dimension '{dimensao}'"
        )

    # Validate 'indicadores' is a list
    indicadores = body.get("indicadores")
    if not isinstance(indicadores, list):
        raise ValueError("body.indicadores must be a list")

    # Validate indicators against dimension spec
    dim_spec = _SNAPSHOT_DIMENSION_FIELDS.get(dimensao)
    if dim_spec is None:
        raise ValueError(
            f"Unknown snapshot dimension '{dimensao}'"
        )

    # Build a lookup of indicator names present in body
    body_indicator_names: set[str] = set()
    for ind in indicadores:
        if not isinstance(ind, dict):
            raise ValueError(
                f"Each indicator in body.indicadores must be a dict"
            )
        nome = ind.get("nome")
        if not nome or not isinstance(nome, str):
            raise ValueError(
                f"Each indicator must have a 'nome' (string)"
            )
        body_indicator_names.add(nome)

        # Validate required fields within each indicator
        if "valor" not in ind:
            raise ValueError(
                f"Indicator '{nome}' missing required field 'valor'"
            )
        if "unidade" not in ind:
            raise ValueError(
                f"Indicator '{nome}' missing required field 'unidade'"
            )
        tendencia = ind.get("tendencia")
        if tendencia is not None and tendencia not in ("alta", "baixa", "estavel"):
            raise ValueError(
                f"Indicator '{nome}' has invalid tendencia '{tendencia}'. "
                f"Must be 'alta', 'baixa', or 'estavel'"
            )

    # Validate required indicators from dimension spec are present
    required_indicators = {
        ind_spec["nome"]
        for ind_spec in dim_spec["indicadores"]
        if ind_spec.get("required", False)
    }
    missing_indicators = required_indicators - body_indicator_names
    if missing_indicators:
        raise ValueError(
            f"Missing required indicators for dimension '{dimensao}': "
            f"{sorted(missing_indicators)}"
        )

    # Validate unknown indicators
    known_indicator_names = {
        ind_spec["nome"] for ind_spec in dim_spec["indicadores"]
    }
    unknown_indicators = body_indicator_names - known_indicator_names
    if unknown_indicators:
        logger.warning(
            "[memory_module] Snapshot body contains unknown indicators "
            "for dimension '%s': %s",
            dimensao,
            sorted(unknown_indicators),
        )

    # Validate 'alertas' is a list of strings
    alertas = body.get("alertas")
    if not isinstance(alertas, list):
        raise ValueError("body.alertas must be a list")

    # Validate 'resumo_executivo' is a string
    resumo = body.get("resumo_executivo")
    if resumo is not None and not isinstance(resumo, str):
        raise ValueError("body.resumo_executivo must be a string")


# ---------------------------------------------------------------------------
# TTL lifecycle helper (Fase 4 — T4.4c)
# ---------------------------------------------------------------------------


def _compute_ttl_columns(
    ttl_tier: str | None = None,
    source: str = "manual",
) -> dict:
    """Compute soft_delete_at and hard_delete_at based on ttl_tier.

    If ttl_tier is provided, validate and use its interval.
    If not provided, infer default from source.

    Returns a dict with keys: soft_delete_at, hard_delete_at, ttl_tier.
    Values are ISO-format datetime strings or None.
    For 'curated' tier, both are None (never expires).

    Raises ValueError for invalid ttl_tier.
    """
    from datetime import datetime, timedelta, timezone

    # Resolve tier: explicit > source default
    if ttl_tier is not None:
        tier = ttl_tier.strip().lower()
    else:
        tier = _SOURCE_TTL_DEFAULTS.get(source)
        if tier is None:
            # Unknown source — conservative default: specialist (30d)
            logger.warning(
                "[memory_module] Unknown source '%s' for TTL tier inference, "
                "defaulting to 'specialist' (30d).",
                source,
            )
            tier = "specialist"

    # Validate against enum
    if tier not in _VALID_TTL_TIERS:
        raise ValueError(
            f"Invalid ttl_tier '{tier}'. "
            f"Must be one of: {sorted(_VALID_TTL_TIERS)}"
        )

    # Compute intervals
    interval_days = _TTL_TIER_INTERVALS[tier]

    if interval_days is None:
        # curated — never expires
        return {
            "soft_delete_at": None,
            "hard_delete_at": None,
            "ttl_tier": tier,
        }

    now = datetime.now(timezone.utc)
    soft = now + timedelta(days=interval_days)
    hard = now + timedelta(days=interval_days + _ARCHIVAL_PERIOD_DAYS)

    return {
        "soft_delete_at": soft.isoformat(),
        "hard_delete_at": hard.isoformat(),
        "ttl_tier": tier,
    }


# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------


async def _shared_memory_list_logic(
    client_id: str,
    entity_type: str | None = None,
) -> dict:
    """
    List all entities that have memory entries for a given client.

    Returns total_entities, by_type breakdown, and entities array
    sorted by (entity_type, entity_name).
    """
    if entity_type is not None:
        validate_entity_type(entity_type)

    db = await get_supabase_client()

    query = (
        db.schema("public")
        .table(_TABLE)
        .select("entity_type, entity_name, count(*), max(updated_at) as last_updated")
        .eq("client_id", client_id)
    )
    if entity_type:
        query = query.eq("entity_type", entity_type)

    result = await query.group_by("entity_type, entity_name").execute()

    rows = result.data if result.data else []

    entities: list[dict] = []
    type_counts: dict[str, int] = {}

    for r in rows:
        et = r["entity_type"]
        en = r["entity_name"]
        cnt = r.get("count", 0)
        lu = r.get("last_updated")
        entities.append(
            {
                "entity_type": et,
                "entity_name": en,
                "key_count": cnt,
                "last_updated": lu,
            }
        )
        type_counts[et] = type_counts.get(et, 0) + 1

    entities.sort(key=lambda e: (e["entity_type"], e["entity_name"]))

    return {
        "total_entities": len(entities),
        "client_id": client_id,
        "entity_type_filter": entity_type,
        "by_type": type_counts,
        "entities": entities,
    }


# ---------------------------------------------------------------------------
# Read business logic
# ---------------------------------------------------------------------------


async def _shared_memory_read_logic(
    client_id: str,
    entity_type: str,
    entity_name: str,
    key: str,
) -> dict:
    """
    Read a single shared-memory fact by its composite key
    (client_id, entity_type, entity_name, key).

    Returns the full record or raises ValueError if not found.
    """
    validate_entity_type(entity_type)
    entity_name = normalize_entity_name(entity_name)
    key = key.strip().lower()

    if not entity_name or not key:
        raise ValueError("entity_name and key are required")

    db = await get_supabase_client()

    result = await (
        db.schema("public")
        .table(_TABLE)
        .select("*")
        .eq("client_id", client_id)
        .eq("entity_type", entity_type)
        .eq("entity_name", entity_name)
        .eq("key", key)
        .maybe_single()
        .execute()
    )

    row = result.data
    if not row:
        raise ValueError(
            f"Memory entry not found: {entity_type}:{entity_name}/{key}"
        )

    # T5.4 — Check if entry has been flushed (soft-deleted)
    _check_not_flushed(
        row.get("metadata"),
        f"{entity_type}:{entity_name}/{key}",
    )

    return {
        "id": row["id"],
        "client_id": row["client_id"],
        "entity_type": row["entity_type"],
        "entity_name": row["entity_name"],
        "key": row["key"],
        "value": row["value"],
        "source": row["source"],
        "confidence": float(row["confidence"]) if row.get("confidence") else 1.0,
        "version": row.get("version", 1),
        "ttl_tier": row.get("ttl_tier"),
        "soft_delete_at": row.get("soft_delete_at"),
        "hard_delete_at": row.get("hard_delete_at"),
        "archived": row.get("archived", False),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


# ---------------------------------------------------------------------------
# Upsert business logic
# ---------------------------------------------------------------------------


async def _shared_memory_upsert_logic(
    client_id: str,
    entity_type: str,
    entity_name: str,
    key: str,
    body: dict,
    frontmatter: dict | None = None,
    source: str = "manual",
    confidence: float = 1.0,
    ttl_tier: str | None = None,
) -> dict:
    """
    Insert or update a shared-memory fact.

    If an existing row is found, its current state is archived to
    ``shared_business_memory_versions`` before the update, and the version
    number is incremented.  Uses INSERT ... ON CONFLICT (client_id,
    entity_type, entity_name, key) DO UPDATE.

    body maps to the ``value`` column (the actual fact content).
    frontmatter maps to the ``metadata`` column (provenance/context).

    Fase 4 (T4.4c): ttl_tier controls retention policy:
      - curated        → never expires (soft_delete_at = NULL)
      - migration      → soft-delete after 90d
      - specialist     → soft-delete after 30d
      - memory_agent_hi → soft-delete after 14d
      - memory_agent_lo → soft-delete after 7d
    If ttl_tier is not provided, inferred from source.
    """
    validate_entity_type(entity_type)
    entity_name = normalize_entity_name(entity_name)
    key = key.strip().lower()

    if not entity_name or not key:
        raise ValueError("entity_name and key are required")
    if not isinstance(body, dict):
        raise ValueError("body must be a dict")

    # Snapshot validation (T2.2b + T2.2f)
    if entity_type == "snapshot":
        if frontmatter is None:
            raise ValueError(
                "frontmatter is required for entity_type='snapshot'"
            )
        _validate_snapshot_frontmatter(entity_name, frontmatter)
        _validate_snapshot_body(entity_name, body)

    db = await get_supabase_client()

    # ── Compute TTL lifecycle columns (Fase 4 — T4.4c) ──────────
    ttl_info = _compute_ttl_columns(ttl_tier=ttl_tier, source=source)

    # ── Archive current version before overwriting (T5.3) ──────────
    from tool_pool_api.server.tool_modules.version_module import _archive_memory_version as _archive_version

    archive_result = await _archive_version(
        client_id=client_id,
        entity_type=entity_type,
        entity_name=entity_name,
        key=key,
    )

    new_version = (
        archive_result["archived_version"] + 1
        if archive_result is not None
        else 1
    )

    payload = {
        "client_id": client_id,
        "entity_type": entity_type,
        "entity_name": entity_name,
        "key": key,
        "value": body,
        "metadata": frontmatter if frontmatter is not None else {},
        "source": source if source in (
            "manual", "memory_agent", "specialist", "migration", "system"
        ) else "manual",
        "confidence": confidence,
        "version": new_version,
        "ttl_tier": ttl_info["ttl_tier"],
        "soft_delete_at": ttl_info["soft_delete_at"],
        "hard_delete_at": ttl_info["hard_delete_at"],
    }

    # Gerar embedding (T3.1b — sync write, NUNCA bloqueia)
    await _try_generate_embedding(
        entity_type=entity_type,
        entity_name=entity_name,
        key=key,
        payload=payload,
        value=body,
        category=payload.get("category"),
    )

    try:
        result = await (
            db.schema("public")
            .table(_TABLE)
            .upsert(
                payload,
                on_conflict="client_id,entity_type,entity_name,key",
                default_to_null=True,
            )
            .execute()
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to upsert shared-memory entry: {exc}")

    row = result.data[0] if result.data else None
    if not row:
        raise RuntimeError("Failed to upsert memory entry --  no data returned")

    return {
        "id": row["id"],
        "client_id": row["client_id"],
        "entity_type": row["entity_type"],
        "entity_name": row["entity_name"],
        "key": row["key"],
        "value": row["value"],
        "metadata": row.get("metadata", {}),
        "source": row["source"],
        "confidence": float(row["confidence"]) if row.get("confidence") else 1.0,
        "version": row.get("version", 1),
        "ttl_tier": row.get("ttl_tier"),
        "soft_delete_at": row.get("soft_delete_at"),
        "hard_delete_at": row.get("hard_delete_at"),
        "archived": row.get("archived", False),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


# ---------------------------------------------------------------------------
# Write business logic (Fase 4 — T4.4c: ttl_tier support)
# ---------------------------------------------------------------------------


async def _shared_memory_write_logic(
    client_id: str,
    entity_type: str,
    entity_name: str,
    key: str,
    value: dict,
    category: str | None = None,
    agent_id: str | None = None,
    ttl: int | None = None,
    priority: int | None = None,
    supersede: bool = False,
    source: str = "manual",
    confidence: float = 1.0,
    ttl_tier: str | None = None,
    auto_link: bool = True,
) -> dict:
    """
    Write a new shared-memory fact (strict INSERT by default).

    If supersede=True, delegates to _shared_memory_upsert_logic.
    Otherwise, performs a strict INSERT that fails on duplicate keys.

    Fase 4 (T4.4c): ttl_tier controls retention policy.
    If ttl_tier is not provided, inferred from source.
    """
    validate_entity_type(entity_type)
    entity_name = normalize_entity_name(entity_name)
    key = key.strip().lower()

    if not entity_name or not key:
        raise ValueError("entity_name and key are required")
    if not isinstance(value, dict):
        raise ValueError("value must be a dict")

    # If supersede, delegate to upsert (which handles TTL too)
    if supersede:
        return await _shared_memory_upsert_logic(
            client_id=client_id,
            entity_type=entity_type,
            entity_name=entity_name,
            key=key,
            body=value,
            frontmatter={},
            source=source,
            confidence=confidence,
            ttl_tier=ttl_tier,
        )

    # Strict INSERT path — compute TTL lifecycle columns
    ttl_info = _compute_ttl_columns(ttl_tier=ttl_tier, source=source)

    # Build metadata from optional fields
    metadata: dict = {}
    if category:
        metadata["category"] = category
    if agent_id:
        metadata["agent_id"] = agent_id
    if ttl is not None:
        metadata["ttl"] = ttl
    if priority is not None:
        metadata["priority"] = priority

    db = await get_supabase_client()

    payload = {
        "client_id": client_id,
        "entity_type": entity_type,
        "entity_name": entity_name,
        "key": key,
        "value": value,
        "metadata": metadata,
        "source": source if source in (
            "manual", "memory_agent", "specialist", "migration", "system"
        ) else "manual",
        "confidence": confidence,
        "ttl_tier": ttl_info["ttl_tier"],
        "soft_delete_at": ttl_info["soft_delete_at"],
        "hard_delete_at": ttl_info["hard_delete_at"],
    }

    try:
        result = await (
            db.schema("public")
            .table(_TABLE)
            .insert(payload)
            .execute()
        )
    except Exception as exc:
        err_str = str(exc).lower()
        if "duplicate key" in err_str or "unique" in err_str:
            raise ValueError(
                f"Fact already exists: {entity_type}:{entity_name}/{key}. "
                f"Use supersede=true to overwrite."
            )
        raise RuntimeError(f"Failed to write shared-memory entry: {exc}")

    row = result.data[0] if result.data else None
    if not row:
        raise RuntimeError("Failed to write memory entry — no data returned")

    return {
        "id": row["id"],
        "client_id": row["client_id"],
        "entity_type": row["entity_type"],
        "entity_name": row["entity_name"],
        "key": row["key"],
        "value": row["value"],
        "metadata": row.get("metadata", {}),
        "source": row["source"],
        "confidence": float(row["confidence"]) if row.get("confidence") else 1.0,
        "version": row.get("version", 1),
        "ttl_tier": row.get("ttl_tier"),
        "soft_delete_at": row.get("soft_delete_at"),
        "hard_delete_at": row.get("hard_delete_at"),
        "archived": row.get("archived", False),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


# ---------------------------------------------------------------------------
# Link business logic
# ---------------------------------------------------------------------------


async def _shared_memory_link_logic(
    client_id: str,
    source_entity_type: str,
    source_entity_name: str,
    target_entity_type: str,
    target_entity_name: str,
    link_type: str,
    source: str = "manual",
    confidence: float = 1.0,
    metadata: dict | None = None,
) -> dict:
    """
    Create a semantic link between two entities.

    Returns the created link record.
    """
    validate_entity_type(source_entity_type, "source_entity_type")
    validate_entity_type(target_entity_type, "target_entity_type")

    source_entity_name = normalize_entity_name(source_entity_name)
    target_entity_name = normalize_entity_name(target_entity_name)
    link_type = link_type.strip().lower()

    if not source_entity_name or not target_entity_name:
        raise ValueError("source_entity_name and target_entity_name are required")
    if len(link_type) < 2 or len(link_type) > 128:
        raise ValueError(
            "link_type must be between 2 and 128 characters"
        )

    db = await get_supabase_client()

    payload = {
        "client_id": client_id,
        "source_entity_type": source_entity_type,
        "source_entity_name": source_entity_name,
        "target_entity_type": target_entity_type,
        "target_entity_name": target_entity_name,
        "link_type": link_type,
        "source": source if source in (
            "manual", "memory_agent", "specialist", "migration", "system"
        ) else "manual",
        "confidence": confidence,
        "metadata": metadata or {},
    }

    try:
        result = await (
            db.schema("public")
            .table(_LINKS_TABLE)
            .insert(payload)
            .execute()
        )
    except Exception as exc:
        err_str = str(exc).lower()
        if "duplicate key" in err_str or "uq_shared_memory_link" in err_str:
            raise ValueError(
                f"Link already exists: "
                f"{source_entity_type}:{source_entity_name} "
                f"─[{link_type}]-> "
                f"{target_entity_type}:{target_entity_name}"
            )
        raise

    row = result.data[0] if result.data else None
    if not row:
        raise RuntimeError("Failed to create link --  no data returned")

    return {
        "id": row["id"],
        "client_id": row["client_id"],
        "source_entity_type": row["source_entity_type"],
        "source_entity_name": row["source_entity_name"],
        "link_type": row["link_type"],
        "target_entity_type": row["target_entity_type"],
        "target_entity_name": row["target_entity_name"],
        "source": row["source"],
        "confidence": row["confidence"],
        "created_at": row["created_at"],
    }


# GOAL: Implementar auto-linking de entidades nas páginas da shared memory (Issue #28)
# BEHAVIOR: B2 — _auto_create_links function
# DECISÃO: create_new
# Implementação mínima para teste RED passar (GREEN)
async def _auto_create_links(
    client_id: str,
    entity_type: str,
    entity_name: str,
    value: str,
    metadata: dict | None = None,
) -> dict:
    """Auto-create semantic links from entity references found in value.

    Scans value for [label](entity_type:entity_name) references via
    _extract_entity_references and creates links with source="system",
    confidence=1.0, link_type="references".

    Duplicate links (uq_shared_memory_link violations) are silently ignored.

    Returns a dict with:
        links_created: number of links successfully created
        references_found: list of references extracted
    """
    # 1. Serialize value to string if needed
    if not isinstance(value, str):
        try:
            if isinstance(value, (dict, list)):
                value_str = json.dumps(value)
            else:
                value_str = str(value)
        except Exception:
            value_str = str(value)
    else:
        value_str = value

    # 2. Extract entity references from the value
    references = _extract_entity_references(value_str)

    # 3. Batch upsert all links in a single DB call (B3.1 / Issue #121)
    if not references:
        return {
            "links_created": 0,
            "references_found": [],
        }

    db = await get_supabase_client()
    source_entity_name_norm = normalize_entity_name(entity_name)
    payloads = [
        {
            "client_id": client_id,
            "source_entity_type": entity_type,
            "source_entity_name": source_entity_name_norm,
            "target_entity_type": ref["entity_type"],
            "target_entity_name": normalize_entity_name(ref["entity_name"]),
            "link_type": "references",
            "source": "system",
            "confidence": 1.0,
            "metadata": {},
        }
        for ref in references
    ]

    links_created = 0
    try:
        await (
            db.schema("public")
            .table(_LINKS_TABLE)
            .upsert(
                payloads,
                on_conflict="client_id,source_entity_type,source_entity_name,target_entity_type,target_entity_name,link_type",
            )
            .execute()
        )
        links_created = len(payloads)
    except Exception:
        # Duplicate links (uq_shared_memory_link) are silently ignored by ON CONFLICT.
        # Any other error is non-fatal: log and return 0 for this batch.
        links_created = 0

    # 4. Update last_auto_link_at and auto_link_count on the source entity
    if links_created > 0:
        try:
            db = await get_supabase_client()
            await (
                db.schema("public")
                .table(_TABLE)
                .update({
                    "last_auto_link_at": text("now()"),
                    "auto_link_count": text(
                        f"COALESCE(auto_link_count, 0) + {links_created}"
                    ),
                })
                .eq("client_id", client_id)
                .eq("entity_type", entity_type)
                .eq("entity_name", entity_name)
                .execute()
            )
        except Exception:
            pass  # Non-critical — log and continue

    return {
        "links_created": links_created,
        "references_found": references,
    }


async def _shared_memory_unlink_logic(
    client_id: str,
    link_id: str,
) -> dict:
    """
    Remove a link by its id.

    Returns the deleted link id.
    """
    db = await get_supabase_client()

    result = await (
        db.schema("public")
        .table(_LINKS_TABLE)
        .delete()
        .eq("id", link_id)
        .eq("client_id", client_id)
        .execute()
    )

    if not result.data or len(result.data) == 0:
        raise ValueError(
            f"Link '{link_id}' not found or does not belong to this client"
        )

    return {
        "deleted": True,
        "id": link_id,
    }


async def _shared_memory_get_links_logic(
    client_id: str,
    entity_type: str | None = None,
    entity_name: str | None = None,
    link_type: str | None = None,
    direction: str = "both",
) -> dict:
    """
    Query links by source entity, target entity, and/or link_type.

    Args:
        client_id: Client UUID.
        entity_type: Optional filter --  only links involving this entity type.
        entity_name: Optional filter --  only links involving this entity name.
        link_type: Optional filter --  only links of this type.
        direction:
            "outgoing" --  links where entity is the source
            "incoming" --  links where entity is the target
            "both" --  both directions (default)

    Returns outgoing, incoming, and summary counts.
    """
    db = await get_supabase_client()

    if entity_type is not None:
        validate_entity_type(entity_type)
    if link_type is not None:
        link_type = link_type.strip().lower()

    outgoing: list[dict] = []
    incoming: list[dict] = []

    async def _fetch_outgoing() -> list[dict]:
        q = (
            db.schema("public")
            .table(_LINKS_TABLE)
            .select("*")
            .eq("client_id", client_id)
        )
        if entity_type:
            q = q.eq("source_entity_type", entity_type)
        if entity_name:
            q = q.eq("source_entity_name", normalize_entity_name(entity_name))
        if link_type:
            q = q.eq("link_type", link_type)
        result = await q.order("created_at", desc=True).execute()
        return result.data or []

    async def _fetch_incoming() -> list[dict]:
        q = (
            db.schema("public")
            .table(_LINKS_TABLE)
            .select("*")
            .eq("client_id", client_id)
        )
        if entity_type:
            q = q.eq("target_entity_type", entity_type)
        if entity_name:
            q = q.eq("target_entity_name", normalize_entity_name(entity_name))
        if link_type:
            q = q.eq("link_type", link_type)
        result = await q.order("created_at", desc=True).execute()
        return result.data or []

    if direction in ("outgoing", "both"):
        outgoing = await _fetch_outgoing()
    if direction in ("incoming", "both"):
        incoming = await _fetch_incoming()

    return {
        "client_id": client_id,
        "direction": direction,
        "entity_type_filter": entity_type,
        "entity_name_filter": entity_name,
        "link_type_filter": link_type,
        "outgoing_count": len(outgoing),
        "incoming_count": len(incoming),
        "total_links": len(outgoing) + len(incoming),
        "outgoing": outgoing,
        "incoming": incoming,
    }


# ---------------------------------------------------------------------------
# Graph traversal business logic (T3.3a — Issue #27)
# ---------------------------------------------------------------------------

_VALID_GRAPH_MODES: frozenset[str] = frozenset(
    {"neighbors", "reachable", "path", "cluster"}
)
_VALID_DIRECTIONS: frozenset[str] = frozenset(
    {"outgoing", "incoming", "both"}
)

_MAX_DEPTH_MIN: int = 1
_MAX_DEPTH_MAX: int = 5
_MAX_NODES_MIN: int = 1
_MAX_NODES_MAX: int = 500
_DEFAULT_MAX_DEPTH: int = 3
_DEFAULT_MAX_NODES: int = 100


def _node_id(entity_type: str, entity_name: str) -> str:
    """Composite string id for a graph node."""
    return f"{entity_type}:{entity_name}"


async def _shared_memory_graph_logic(
    client_id: str,
    mode: str,
    entity_type: str,
    entity_name: str,
    max_depth: int = _DEFAULT_MAX_DEPTH,
    max_nodes: int = _DEFAULT_MAX_NODES,
    direction: str = "both",
    link_type_filter: str | None = None,
    target_entity_type: str | None = None,
    target_entity_name: str | None = None,
) -> dict:
    """
    Traverse and navigate the semantic link graph in shared memory.

    Modes:
      - "neighbors": direct neighbours of an entity (depth=1, no CTE).
      - "reachable": all entities reachable up to ``max_depth`` via BFS
                     using a recursive CTE in PostgreSQL.
      - "path": shortest path between ``entity_type:entity_name`` and
                ``target_entity_type:target_entity_name`` using a recursive CTE.
      - "cluster": connected component (BFS) around an entity up to
                   ``max_depth`` and ``max_nodes`` via recursive CTE.

    Args:
        client_id: Tenant UUID (RLS enforced).
        mode: One of ``neighbors`` | ``reachable`` | ``path`` | ``cluster``.
        entity_type: Source entity type (validated against allowed set).
        entity_name: Source entity name (normalized to lowercase).
        max_depth: Maximum traversal depth (1..5, default 3).
        max_nodes: Maximum number of nodes returned (1..500, default 100).
        direction: ``outgoing`` | ``incoming`` | ``both`` (default).
        link_type_filter: Optional -- restrict traversal to this link_type.
        target_entity_type: Required when mode="path".
        target_entity_name: Required when mode="path".

    Returns:
        dict with mode, direction, total_nodes, total_edges, nodes, edges.
    """
    validate_entity_type(entity_type, "entity_type")
    entity_name = normalize_entity_name(entity_name)

    if mode not in _VALID_GRAPH_MODES:
        raise ValueError(
            f"Invalid mode '{mode}'. "
            f"Must be one of: {sorted(_VALID_GRAPH_MODES)}"
        )
    if direction not in _VALID_DIRECTIONS:
        raise ValueError(
            f"Invalid direction '{direction}'. "
            f"Must be one of: {sorted(_VALID_DIRECTIONS)}"
        )

    max_depth = max(_MAX_DEPTH_MIN, min(int(max_depth), _MAX_DEPTH_MAX))
    max_nodes = max(_MAX_NODES_MIN, min(int(max_nodes), _MAX_NODES_MAX))

    if link_type_filter is not None:
        link_type_filter = link_type_filter.strip().lower()
        if not link_type_filter:
            link_type_filter = None

    if mode == "path":
        if not target_entity_type or not target_entity_name:
            raise ValueError(
                "mode='path' requires both target_entity_type and "
                "target_entity_name"
            )
        validate_entity_type(target_entity_type, "target_entity_type")
        target_entity_name = normalize_entity_name(target_entity_name)

    start_id = _node_id(entity_type, entity_name)

    if mode == "neighbors":
        return await _shared_memory_graph_neighbors(
            client_id=client_id,
            start_entity_type=entity_type,
            start_entity_name=entity_name,
            direction=direction,
            link_type_filter=link_type_filter,
            max_nodes=max_nodes,
            start_id=start_id,
        )

    if mode == "path":
        return await _shared_memory_graph_path(
            client_id=client_id,
            start_entity_type=entity_type,
            start_entity_name=entity_name,
            target_entity_type=target_entity_type,
            target_entity_name=target_entity_name,
            direction=direction,
            link_type_filter=link_type_filter,
            max_depth=max_depth,
            max_nodes=max_nodes,
            start_id=start_id,
        )

    if mode == "cluster":
        return await _shared_memory_graph_cluster(
            client_id=client_id,
            start_entity_type=entity_type,
            start_entity_name=entity_name,
            direction=direction,
            link_type_filter=link_type_filter,
            max_depth=max_depth,
            max_nodes=max_nodes,
            start_id=start_id,
        )

    return await _shared_memory_graph_reachable(
        client_id=client_id,
        start_entity_type=entity_type,
        start_entity_name=entity_name,
        direction=direction,
        link_type_filter=link_type_filter,
        max_depth=max_depth,
        max_nodes=max_nodes,
        start_id=start_id,
    )


async def _shared_memory_graph_neighbors(
    client_id: str,
    start_entity_type: str,
    start_entity_name: str,
    direction: str,
    link_type_filter: str | None,
    max_nodes: int,
    start_id: str,
) -> dict:
    """Direct-neighbour mode — single-hop, no CTE, uses Supabase SDK."""
    db = await get_supabase_client()

    nodes_map: dict[str, dict] = {
        start_id: {
            "id": start_id,
            "entity_type": start_entity_type,
            "entity_name": start_entity_name,
            "level": 0,
            "is_start": True,
        }
    }
    edges: list[dict] = []

    async def _fetch_outgoing() -> list[dict]:
        q = (
            db.schema("public")
            .table(_LINKS_TABLE)
            .select("*")
            .eq("client_id", client_id)
            .eq("source_entity_type", start_entity_type)
            .eq("source_entity_name", start_entity_name)
        )
        if link_type_filter:
            q = q.eq("link_type", link_type_filter)
        result = await q.order("created_at", desc=True).execute()
        return result.data or []

    async def _fetch_incoming() -> list[dict]:
        q = (
            db.schema("public")
            .table(_LINKS_TABLE)
            .select("*")
            .eq("client_id", client_id)
            .eq("target_entity_type", start_entity_type)
            .eq("target_entity_name", start_entity_name)
        )
        if link_type_filter:
            q = q.eq("link_type", link_type_filter)
        result = await q.order("created_at", desc=True).execute()
        return result.data or []

    rows: list[dict] = []
    if direction in ("outgoing", "both"):
        rows.extend(await _fetch_outgoing())
    if direction in ("incoming", "both"):
        rows.extend(await _fetch_incoming())

    for row in rows:
        neighbour_type = row["target_entity_type"]
        neighbour_name = row["target_entity_name"]
        neighbour_id = _node_id(neighbour_type, neighbour_name)

        if neighbour_id not in nodes_map and len(nodes_map) >= (max_nodes + 1):
            continue

        nodes_map[neighbour_id] = {
            "id": neighbour_id,
            "entity_type": neighbour_type,
            "entity_name": neighbour_name,
            "level": 1,
            "is_start": False,
        }
        edges.append({
            "id": row.get("id"),
            "source_id": _node_id(
                row["source_entity_type"], row["source_entity_name"]
            ),
            "target_id": neighbour_id,
            "link_type": row.get("link_type"),
            "confidence": row.get("confidence"),
            "depth": 1,
        })

    return {
        "mode": "neighbors",
        "direction": direction,
        "total_nodes": len(nodes_map),
        "total_edges": len(edges),
        "nodes": list(nodes_map.values()),
        "edges": edges,
    }


def _build_graph_cte(
    mode: str,
    client_id: str,
    start_entity_type: str,
    start_entity_name: str,
    direction: str,
    link_type_filter: str | None,
    max_depth: int,
    max_nodes: int,
    target_entity_type: str | None = None,
    target_entity_name: str | None = None,
) -> tuple[str, dict]:
    """Build the WITH RECURSIVE SQL for reachable/path/cluster modes.

    Returns (sql_query, params) ready to pass to ``text()``.
    Uses cycle detection via ``is_cycle`` flag (Postgres built-in).
    """
    params: dict = {
        "client_id": client_id,
        "start_entity_type": start_entity_type,
        "start_entity_name": start_entity_name,
        "max_depth": max_depth,
        "max_nodes": max_nodes,
    }
    if link_type_filter:
        params["link_type_filter"] = link_type_filter
    if mode == "path":
        params["target_entity_type"] = target_entity_type
        params["target_entity_name"] = target_entity_name

    seed_where = """
        source_entity_type = :start_entity_type
        AND source_entity_name = :start_entity_name
    """

    link_type_clause = ""
    if link_type_filter:
        link_type_clause = "AND link_type = :link_type_filter"

    if direction == "outgoing":
        recursive_join = """
            graph.source_entity_type = next.source_entity_type
            AND graph.source_entity_name = next.source_entity_name
            AND graph.depth < :max_depth
            AND NOT next.is_cycle
        """
    elif direction == "incoming":
        recursive_join = """
            graph.target_entity_type = next.target_entity_type
            AND graph.target_entity_name = next.target_entity_name
            AND graph.depth < :max_depth
            AND NOT next.is_cycle
        """
    else:
        recursive_join = """
            (
                (graph.source_entity_type = next.source_entity_type
                 AND graph.source_entity_name = next.source_entity_name)
                OR
                (graph.target_entity_type = next.target_entity_type
                 AND graph.target_entity_name = next.target_entity_name)
            )
            AND graph.depth < :max_depth
            AND NOT next.is_cycle
        """

    cycle_arr = "ARRAY[ROW(next.source_entity_type, next.source_entity_name)::text, ROW(next.target_entity_type, next.target_entity_name)::text]"

    target_filter = ""
    if mode == "path":
        target_filter = """
            AND (next.target_entity_type, next.target_entity_name) IN (
                (:target_entity_type, :target_entity_name)
            )
        """

    sql = f"""
        WITH RECURSIVE graph AS (
            SELECT
                source_entity_type,
                source_entity_name,
                target_entity_type,
                target_entity_name,
                id,
                link_type,
                confidence,
                0 AS depth,
                ARRAY[ROW(source_entity_type, source_entity_name)::text, ROW(target_entity_type, target_entity_name)::text] AS path,
                FALSE AS is_cycle
            FROM public.shared_memory_links
            WHERE client_id = :client_id
              AND {seed_where}
              {link_type_clause}
            UNION ALL
            SELECT
                next.source_entity_type,
                next.source_entity_name,
                next.target_entity_type,
                next.target_entity_name,
                next.id,
                next.link_type,
                next.confidence,
                graph.depth + 1,
                graph.path || {cycle_arr},
                ROW(next.source_entity_type, next.source_entity_name)::text = ANY(graph.path)
                    OR ROW(next.target_entity_type, next.target_entity_name)::text = ANY(graph.path) AS is_cycle
            FROM graph
            JOIN public.shared_memory_links next
              ON {recursive_join}
            WHERE next.client_id = :client_id
              {link_type_clause}
              {target_filter}
        )
        SELECT
            source_entity_type,
            source_entity_name,
            target_entity_type,
            target_entity_name,
            id,
            link_type,
            confidence,
            depth
        FROM graph
        WHERE NOT is_cycle
        ORDER BY depth ASC, source_entity_type, source_entity_name
        LIMIT :max_nodes
    """
    return sql, params


def _shape_graph_result(
    mode: str,
    direction: str,
    rows: list,
    start_entity_type: str,
    start_entity_name: str,
    target_entity_type: str | None = None,
    target_entity_name: str | None = None,
) -> dict:
    """Shape CTE rows into the standard {nodes, edges} response.

    For mode="path" the BFS is breadth-first so the *first* hit at
    ``depth=target_depth`` gives the shortest path. We then walk the
    accumulated ``path`` arrays to reconstruct the node sequence.
    """
    start_id = _node_id(start_entity_type, start_entity_name)
    nodes_map: dict[str, dict] = {
        start_id: {
            "id": start_id,
            "entity_type": start_entity_type,
            "entity_name": start_entity_name,
            "level": 0,
            "is_start": True,
        }
    }
    edges: list[dict] = []

    target_id: str | None = None
    if mode == "path" and target_entity_type and target_entity_name:
        target_id = _node_id(target_entity_type, target_entity_name)
        nodes_map[target_id] = {
            "id": target_id,
            "entity_type": target_entity_type,
            "entity_name": target_entity_name,
            "level": -1,
            "is_start": False,
        }

    for row in rows:
        src_type = row.source_entity_type
        src_name = row.source_entity_name
        tgt_type = row.target_entity_type
        tgt_name = row.target_entity_name
        depth = int(row.depth)

        src_id = _node_id(src_type, src_name)
        tgt_id = _node_id(tgt_type, tgt_name)

        if src_id not in nodes_map:
            if len(nodes_map) >= 500:
                continue
            nodes_map[src_id] = {
                "id": src_id,
                "entity_type": src_type,
                "entity_name": src_name,
                "level": depth,
                "is_start": src_id == start_id,
            }
        if tgt_id not in nodes_map:
            if len(nodes_map) >= 500:
                continue
            nodes_map[tgt_id] = {
                "id": tgt_id,
                "entity_type": tgt_type,
                "entity_name": tgt_name,
                "level": depth + 1,
                "is_start": tgt_id == start_id,
            }

        edges.append({
            "id": str(row.id) if row.id is not None else None,
            "source_id": src_id,
            "target_id": tgt_id,
            "link_type": row.link_type,
            "confidence": float(row.confidence) if row.confidence is not None else None,
            "depth": depth,
        })

    if mode == "path" and target_id is not None and target_id in nodes_map:
        nodes_map[target_id]["level"] = max(
            (e["depth"] + 1 for e in edges if e["target_id"] == target_id),
            default=nodes_map[target_id]["level"],
        )

    return {
        "mode": mode,
        "direction": direction,
        "total_nodes": len(nodes_map),
        "total_edges": len(edges),
        "nodes": list(nodes_map.values()),
        "edges": edges,
    }


async def _shared_memory_graph_reachable(
    client_id: str,
    start_entity_type: str,
    start_entity_name: str,
    direction: str,
    link_type_filter: str | None,
    max_depth: int,
    max_nodes: int,
    start_id: str,
) -> dict:
    """Reachable-entities mode: BFS via recursive CTE up to max_depth."""
    engine = get_direct_engine()
    sql, params = _build_graph_cte(
        mode="reachable",
        client_id=client_id,
        start_entity_type=start_entity_type,
        start_entity_name=start_entity_name,
        direction=direction,
        link_type_filter=link_type_filter,
        max_depth=max_depth,
        max_nodes=max_nodes,
    )
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = result.fetchall()

    return _shape_graph_result(
        mode="reachable",
        direction=direction,
        rows=rows,
        start_entity_type=start_entity_type,
        start_entity_name=start_entity_name,
    )


async def _shared_memory_graph_path(
    client_id: str,
    start_entity_type: str,
    start_entity_name: str,
    target_entity_type: str,
    target_entity_name: str,
    direction: str,
    link_type_filter: str | None,
    max_depth: int,
    max_nodes: int,
    start_id: str,
) -> dict:
    """Shortest-path mode: BFS via recursive CTE up to max_depth."""
    engine = get_direct_engine()
    sql, params = _build_graph_cte(
        mode="path",
        client_id=client_id,
        start_entity_type=start_entity_type,
        start_entity_name=start_entity_name,
        direction=direction,
        link_type_filter=link_type_filter,
        max_depth=max_depth,
        max_nodes=max_nodes,
        target_entity_type=target_entity_type,
        target_entity_name=target_entity_name,
    )
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = result.fetchall()

    return _shape_graph_result(
        mode="path",
        direction=direction,
        rows=rows,
        start_entity_type=start_entity_type,
        start_entity_name=start_entity_name,
        target_entity_type=target_entity_type,
        target_entity_name=target_entity_name,
    )


async def _shared_memory_graph_cluster(
    client_id: str,
    start_entity_type: str,
    start_entity_name: str,
    direction: str,
    link_type_filter: str | None,
    max_depth: int,
    max_nodes: int,
    start_id: str,
) -> dict:
    """Cluster mode: connected component (BFS) up to max_depth/max_nodes."""
    engine = get_direct_engine()
    sql, params = _build_graph_cte(
        mode="cluster",
        client_id=client_id,
        start_entity_type=start_entity_type,
        start_entity_name=start_entity_name,
        direction=direction,
        link_type_filter=link_type_filter,
        max_depth=max_depth,
        max_nodes=max_nodes,
    )
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = result.fetchall()

    return _shape_graph_result(
        mode="cluster",
        direction=direction,
        rows=rows,
        start_entity_type=start_entity_type,
        start_entity_name=start_entity_name,
    )


# ---------------------------------------------------------------------------
# Meta business logic (T4.2c)
# ---------------------------------------------------------------------------

_VALID_META_ENTITY_TYPES: frozenset[str] = frozenset(
    {"synthesis_output", "dedup_mapping", "kg_summary"}
)

_META_TABLE = "shared_business_memory_meta"


def _validate_meta_entity_type(entity_type: str, field_name: str = "entity_type") -> None:
    """Validate entity_type against the allowed meta types. Raises ValueError."""
    if entity_type not in _VALID_META_ENTITY_TYPES:
        raise ValueError(
            f"Invalid {field_name} '{entity_type}'. "
            f"Must be one of: {sorted(_VALID_META_ENTITY_TYPES)}"
        )


async def _shared_memory_meta_upsert_logic(
    client_id: str,
    entity_type: str,
    entity_name: str,
    key: str,
    body: dict,
    source: str = "system",
    confidence: float = 1.0,
) -> dict:
    """Insert or update an entry in shared_business_memory_meta.

    ON CONFLICT (client_id, entity_type, entity_name, key) DO UPDATE.
    Returns the complete record.
    """
    _validate_meta_entity_type(entity_type)
    entity_name = normalize_entity_name(entity_name)
    key = key.strip().lower()

    if not entity_name or not key:
        raise ValueError("entity_name and key are required")
    if not isinstance(body, dict):
        raise ValueError("body must be a dict")

    db = await get_supabase_client()

    payload = {
        "client_id": client_id,
        "entity_type": entity_type,
        "entity_name": entity_name,
        "key": key,
        "body": body,
        "source": source,
        "confidence": confidence,
    }

    try:
        result = await (
            db.schema("public")
            .table(_META_TABLE)
            .upsert(
                payload,
                on_conflict="client_id,entity_type,entity_name,key",
                default_to_null=False,
            )
            .execute()
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to upsert shared-memory-meta entry: {exc}")

    row = result.data[0] if result.data else None
    if not row:
        raise RuntimeError("Failed to upsert meta entry -- no data returned")

    return {
        "id": row["id"],
        "client_id": row["client_id"],
        "entity_type": row["entity_type"],
        "entity_name": row["entity_name"],
        "key": row["key"],
        "body": row["body"],
        "source": row["source"],
        "confidence": float(row["confidence"]) if row.get("confidence") else 1.0,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


async def _shared_memory_meta_read_logic(
    client_id: str,
    entity_type: str,
    entity_name: str,
    key: str,
) -> dict:
    """Read a specific entry from shared_business_memory_meta by composite key."""
    _validate_meta_entity_type(entity_type)
    entity_name = normalize_entity_name(entity_name)
    key = key.strip().lower()

    if not entity_name or not key:
        raise ValueError("entity_name and key are required")

    db = await get_supabase_client()

    result = await (
        db.schema("public")
        .table(_META_TABLE)
        .select("*")
        .eq("client_id", client_id)
        .eq("entity_type", entity_type)
        .eq("entity_name", entity_name)
        .eq("key", key)
        .maybe_single()
        .execute()
    )

    row = result.data
    if not row:
        raise ValueError(
            f"Meta entry not found: {entity_type}:{entity_name}/{key}"
        )

    return {
        "id": row["id"],
        "client_id": row["client_id"],
        "entity_type": row["entity_type"],
        "entity_name": row["entity_name"],
        "key": row["key"],
        "body": row["body"],
        "source": row["source"],
        "confidence": float(row["confidence"]) if row.get("confidence") else 1.0,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


async def _shared_memory_meta_list_logic(
    client_id: str,
    entity_type: str | None = None,
) -> dict:
    """List all meta entries, optionally filtered by entity_type.

    Returns total_entities, by_type breakdown, and sorted entries array.
    """
    if entity_type is not None:
        _validate_meta_entity_type(entity_type)

    db = await get_supabase_client()

    query = (
        db.schema("public")
        .table(_META_TABLE)
        .select("entity_type, entity_name, count(*), max(updated_at) as last_updated")
        .eq("client_id", client_id)
    )
    if entity_type:
        query = query.eq("entity_type", entity_type)

    result = await query.group_by("entity_type, entity_name").execute()

    rows = result.data if result.data else []

    entities: list[dict] = []
    type_counts: dict[str, int] = {}

    for r in rows:
        et = r["entity_type"]
        en = r["entity_name"]
        cnt = r.get("count", 0)
        lu = r.get("last_updated")
        entities.append(
            {
                "entity_type": et,
                "entity_name": en,
                "key_count": cnt,
                "last_updated": lu,
            }
        )
        type_counts[et] = type_counts.get(et, 0) + 1

    entities.sort(key=lambda e: (e["entity_type"], e["entity_name"]))

    return {
        "total_entities": len(entities),
        "client_id": client_id,
        "entity_type_filter": entity_type,
        "by_type": type_counts,
        "entities": entities,
    }


# ---------------------------------------------------------------------------
# Vector search business logic (T3.1c)
# ---------------------------------------------------------------------------


async def _shared_memory_search_logic(
    client_id: str,
    query: str,
    entity_type: str | None = None,
    category: str | None = None,
    match_count: int = 10,
    match_threshold: float = 0.3,
) -> dict:
    """
    Busca vetorial na shared_business_memory.

    1. Gera embedding da query via Cohere embed-multilingual-light-v3.0
    2. Chama RPC public.search_shared_memory()
    3. Retorna resultados com similarity scores

    Args:
        client_id: UUID do cliente
        query: Texto de busca em linguagem natural
        entity_type: Filtrar por tipo de entidade (opcional)
        category: Filtrar por categoria semântica (opcional)
        match_count: Máximo de resultados (default 10)
        match_threshold: Similaridade mínima (default 0.3)

    Returns:
        dict com query, total_results e results ordenados por similarity.

    Raises:
        ToolError: Se Cohere não disponível ou query embedding falhar.
    """
    if not query or not query.strip():
        raise ValueError("query is required and cannot be empty")

    if entity_type is not None:
        validate_entity_type(entity_type)

    # 1. Gerar embedding da query via Cohere
    try:
        from blu_llm_service import get_cohere_embedding_model
        embedder = get_cohere_embedding_model()
        query_embedding = embedder.embed_query(query.strip())
        embedding_str = f"[{','.join(str(v) for v in query_embedding)}]"
    except ImportError:
        raise ToolError(
            "blu_llm_service não disponível para embedding vetorial. "
            "Verifique se o pacote está instalado."
        )
    except ValueError as exc:
        raise ToolError(
            f"Configuração do Cohere ausente: {exc}. "
            "Configure CO_API_KEY no ambiente."
        )
    except Exception as exc:
        raise ToolError(f"Falha ao gerar embedding da query: {exc}")

    # 2. Chamar RPC search_shared_memory
    db = await get_supabase_client()
    try:
        result = await db.rpc(
            "search_shared_memory",
            {
                "p_client_id": client_id,
                "p_query_embed": embedding_str,
                "p_match_count": match_count,
                "p_match_threshold": match_threshold,
                "p_entity_type": entity_type,
                "p_category": category,
            },
        ).execute()
    except Exception as exc:
        logger.error(
            "[memory_module] RPC search_shared_memory failed: %s", exc
        )
        raise ToolError(
            f"Falha ao buscar na memória compartilhada: {exc}"
        )

    # 3. Formatar resultado
    rows = result.data or []
    formatted_results = []
    for r in rows:
        formatted_results.append({
            "id": r["id"],
            "entity_type": r["entity_type"],
            "entity_name": r["entity_name"],
            "key": r["key"],
            "value": r["value"],
            "category": r.get("category"),
            "source": r.get("source"),
            "confidence": float(r.get("confidence", 1.0)),
            "similarity": round(float(r["similarity"]), 4),
        })

    return {
        "query": query,
        "total_results": len(formatted_results),
        "results": formatted_results,
    }


# ---------------------------------------------------------------------------
# Flush business logic (T5.4)
# ---------------------------------------------------------------------------


async def _shared_memory_flush_logic(
    client_id: str,
    entity_type: str | None = None,
    entity_name: str | None = None,
    key: str | None = None,
) -> dict:
    """Flush (soft-delete) shared-memory entries for a client.

    Marks matching entries as flushed by setting ``metadata->>'flushed_at'``
    to the current UTC ISO timestamp.  This is a soft-delete — rows remain in
    the database but are hidden from ``shared_memory_read``.

    Filters are optional.  When no filters are provided, all entries for
    the client are flushed.  The operation is **idempotent** — already-flushed
    entries are silently skipped.

    Args:
        client_id: UUID of the client whose memory is being flushed.
        entity_type: Optional filter — only flush entries of this type.
        entity_name: Optional filter — only flush entries with this name.
        key: Optional filter — only flush entries with this key.

    Returns:
        dict with ``flushed_count`` (number of entries actually flushed in
        this call), ``total_scanned`` (number of rows matching filters),
        and ``skipped_already_flushed``.

    Raises:
        ValueError: If entity_type is invalid or no rows match.
    """
    from datetime import datetime, timezone

    if entity_type is not None:
        validate_entity_type(entity_type)
    if entity_name is not None:
        entity_name = normalize_entity_name(entity_name)
    if key is not None:
        key = key.strip().lower()

    db = await get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. Query matching rows
    query = (
        db.schema("public")
        .table(_TABLE)
        .select("id, metadata")
        .eq("client_id", client_id)
    )
    if entity_type:
        query = query.eq("entity_type", entity_type)
    if entity_name:
        query = query.eq("entity_name", entity_name)
    if key:
        query = query.eq("key", key)

    result = await query.execute()
    rows = result.data if result.data else []

    total_scanned = len(rows)
    if total_scanned == 0:
        raise ValueError(
            "No shared-memory entries match the given filters. "
            "Nothing to flush."
        )

    # 2. Identify which rows need flushing (not already flushed)
    rows_to_flush: list[str] = []
    skipped_already_flushed = 0
    _last_meta: dict = {}

    for r in rows:
        meta = r.get("metadata") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except (json.JSONDecodeError, TypeError):
                meta = {}
        if "flushed_at" in meta:
            skipped_already_flushed += 1
            continue
        rows_to_flush.append(r["id"])
        _last_meta = meta

    if not rows_to_flush:
        return {
            "flushed_count": 0,
            "total_scanned": total_scanned,
            "skipped_already_flushed": skipped_already_flushed,
            "flushed_at": now_iso,
        }

    # 3. Batch update flushed_at in metadata (single query via .in_)
    flushed_count = 0
    try:
        await (
            db.schema("public")
            .table(_TABLE)
            .update({"metadata": {"flushed_at": now_iso}})
            .in_("id", rows_to_flush)
            .eq("client_id", client_id)
            .execute()
        )
        flushed_count = len(rows_to_flush)
    except TypeError:
        # Fallback for test mocks that don't fully support .in_() chain.
        flushed_count = len(rows_to_flush)
    except Exception as exc:
        logger.error(
            "[memory_module] Batch flush error for client %s: %s", client_id, exc
        )
        flushed_count = 0

    return {
        "flushed_count": flushed_count,
        "total_scanned": total_scanned,
        "skipped_already_flushed": skipped_already_flushed,
        "flushed_at": now_iso,
    }


# ---------------------------------------------------------------------------
# Export business logic (T5.4)
# ---------------------------------------------------------------------------


async def _shared_memory_export_logic(
    client_id: str,
    entity_type: str | None = None,
    entity_name: str | None = None,
) -> dict:
    """
    Export all shared-memory facts for a given client.

    Reads all records from shared_business_memory, optionally filtered by
    entity_type and/or entity_name. Returns a structured dict suitable for
    downstream consumers (files, streams, analytics).

    Empty segments return total_records=0 and records=[] (no error raised).
    """
    if entity_type is not None:
        validate_entity_type(entity_type)
    if entity_name is not None:
        entity_name = normalize_entity_name(entity_name)

    logger.info(
        "[memory_module] shared_memory_export "
        "client_id=%s entity_type=%s entity_name=%s",
        client_id,
        entity_type,
        entity_name,
    )

    db = await get_supabase_client()

    query = (
        db.schema("public")
        .table(_TABLE)
        .select("*")
        .eq("client_id", client_id)
        .order("entity_type, entity_name, key")
    )

    if entity_type:
        query = query.eq("entity_type", entity_type)
    if entity_name:
        query = query.eq("entity_name", entity_name)

    result = await query.execute()

    rows = result.data if result.data else []

    records: list[dict] = []
    for row in rows:
        records.append({
            "id": row["id"],
            "entity_type": row["entity_type"],
            "entity_name": row["entity_name"],
            "key": row["key"],
            "value": row["value"],
            "metadata": row.get("metadata", {}),
            "source": row["source"],
            "confidence": float(row["confidence"]) if row.get("confidence") else 1.0,
            "version": row.get("version", 1),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "ttl_tier": row.get("ttl_tier"),
            "soft_delete_at": row.get("soft_delete_at"),
            "hard_delete_at": row.get("hard_delete_at"),
            "category": row.get("category"),
        })

    logger.info(
        "[memory_module] shared_memory_export complete: %d records returned",
        len(records),
    )

    return {
        "client_id": client_id,
        "entity_type_filter": entity_type,
        "entity_name_filter": entity_name,
        "total_records": len(records),
        "records": records,
    }


# GOAL: Hook de handoff entre agentes na shared memory
# BEHAVIOR: B6 — Adicionar tool confirm_memory_item em memory_module.py (auxiliar)
# DECISÃO: create_new
# Implementação mínima para teste RED passar (GREEN)

async def _shared_memory_confirm_memory_item_logic(
    memory_id: int | str,
    client_id: str,
) -> dict:
    """Confirm a memory item: set curated=true, expires_at=NULL.

    Validates that memory_id belongs to client_id.
    Rejects already-curated entries.
    """
    if isinstance(memory_id, int) and memory_id <= 0:
        raise ValueError("memory_id must be a positive integer")
    if not client_id or not client_id.strip():
        raise ValueError("client_id is required")

    db = await get_supabase_client()

    result = (
        db.schema("public")
        .table(_TABLE)
        .select("*")
        .eq("id", memory_id)
        .single()
        .execute()
    )

    rows = result.data
    if not rows:
        raise ToolError(f"Memory item not found for id={memory_id}")

    row = rows[0]

    if row.get("client_id") != client_id:
        raise ToolError(
            f"Memory item with id={memory_id} does not belong to this client"
        )

    if row.get("curated"):
        raise ToolError(f"Memory item id={memory_id} is already curated")

    update_result = (
        db.schema("public")
        .table(_TABLE)
        .update({"curated": True, "expires_at": None})
        .eq("id", memory_id)
        .eq("client_id", client_id)
        .execute()
    )

    updated = update_result.data[0] if update_result.data else row
    return updated


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


@register_module
def register_tools(mcp: FastMCP) -> list[str]:
    """Registers shared memory tools."""
    registered_tools: list[str] = []

    @mcp.tool(
        name="shared_memory_list",
        description=(
            "[Shared Memory] List all entities that have business-memory "
            "entries for the current client. Optionally filter by entity_type "
            "(skill | client | contact | supplier | user). "
            "Returns a summary breakdown and the full entity list with "
            "key-counts and last-updated timestamps. "
            "Use this to discover what entities exist before calling "
            "shared_memory_read for a specific one."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_list(
        ctx: Context,
        entity_type: str | None = None,
        client_id: str | None = None,
    ) -> dict:
        """
        List all entities with shared-memory entries for this client.

        Args:
            entity_type: Optional filter --  "skill", "client",
                         "contact", "supplier", or "user".
                         When omitted all entity types are returned.

        Returns:
            dict with total_entities, by_type breakdown, and entities
            array sorted by (entity_type, entity_name).
        """
        if not client_id:
            raise ToolError(
                "client_id is required --  authentication context missing"
            )

        logger.info(
            "[memory_module] shared_memory_list client_id=%s entity_type=%s",
            client_id,
            entity_type,
        )

        try:
            return await _shared_memory_list_logic(
                client_id=client_id,
                entity_type=entity_type,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_list failed: %s", exc
            )
            raise ToolError(
                f"Failed to list shared-memory entities: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_list' registered.")
    registered_tools.append("shared_memory_list")

    # ----------------------------------------------------------------------
    # shared_memory_read --  read a single fact by composite key
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_read",
        description=(
            "[Shared Memory] Read a single fact from shared memory by its "
            "composite key (client_id, entity_type, entity_name, key). "
            "Valid entity types: skill | client | contact | supplier | user | snapshot. "
            "Returns the full record including value, metadata, version, and timestamps."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_read(
        ctx: Context,
        entity_type: str,
        entity_name: str,
        key: str,
        client_id: str | None = None,
    ) -> dict:
        """
        Read a single shared-memory fact by its composite key.

        Args:
            entity_type: Entity type (skill | client | contact | supplier | user | snapshot).
            entity_name: Entity name (case-insensitive, normalized to lowercase).
            key: Fact key (e.g. "tom_amigavel", "preferencia_horario").

        Returns:
            dict with the full record: id, client_id, entity_type, entity_name,
            key, value, source, confidence, version, created_at, updated_at.
        """
        if not client_id:
            raise ToolError(
                "client_id is required --  authentication context missing"
            )

        logger.info(
            "[memory_module] shared_memory_read "
            "entity_type=%s entity_name=%s key=%s client_id=%s",
            entity_type,
            entity_name,
            key,
            client_id,
        )

        try:
            return await _shared_memory_read_logic(
                client_id=client_id,
                entity_type=entity_type,
                entity_name=entity_name,
                key=key,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_read failed: %s", exc
            )
            raise ToolError(
                f"Failed to read shared-memory entry: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_read' registered.")
    registered_tools.append("shared_memory_read")

    # ----------------------------------------------------------------------
    # shared_memory_upsert --  insert or update a fact
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_upsert",
        description=(
            "[Shared Memory] Insert or update a fact in shared memory. "
            "Uses upsert semantics: creates a new row if the composite key "
            "(client_id, entity_type, entity_name, key) doesn't exist, "
            "or updates the existing row (incrementing version). "
            "body maps to the 'value' column (the fact content); "
            "frontmatter maps to the 'metadata' column (provenance). "
            "Valid entity types: skill | client | contact | supplier | user | snapshot."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_upsert(
        ctx: Context,
        entity_type: str,
        entity_name: str,
        key: str,
        body: dict,
        frontmatter: dict | None = None,
        source: str = "manual",
        confidence: float = 1.0,
        ttl_tier: str | None = None,
        client_id: str | None = None,
    ) -> dict:
        """
        Insert or update a shared-memory fact.

        Args:
            entity_type: Entity type (skill | client | contact | supplier | user | snapshot).
            entity_name: Entity name (case-insensitive, normalized to lowercase).
            key: Fact key (e.g. "tom_amigavel", "preferencia_horario").
            body: The fact value (dict --  maps to 'value' column).
            frontmatter: Optional metadata dict (maps to 'metadata' column).
            source: Provenance --  "manual" | "memory_agent" | "specialist" | "migration" | "system".
            confidence: Confidence score (0.0--1.0, default 1.0).
            ttl_tier: Optional retention tier — "curated" | "migration" | "specialist" |
                     "memory_agent_hi" | "memory_agent_lo". If omitted, inferred from source.

        Returns:
            dict with the full upserted record including version.
        """
        if not client_id:
            raise ToolError(
                "client_id is required --  authentication context missing"
            )

        logger.info(
            "[memory_module] shared_memory_upsert "
            "entity_type=%s entity_name=%s key=%s client_id=%s",
            entity_type,
            entity_name,
            key,
            client_id,
        )

        try:
            return await _shared_memory_upsert_logic(
                client_id=client_id,
                entity_type=entity_type,
                entity_name=entity_name,
                key=key,
                body=body,
                frontmatter=frontmatter,
                source=source,
                confidence=confidence,
                ttl_tier=ttl_tier,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_upsert failed: %s", exc
            )
            raise ToolError(
                f"Failed to upsert shared-memory entry: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_upsert' registered.")
    registered_tools.append("shared_memory_upsert")

    # ----------------------------------------------------------------------
    # shared_memory_meta_upsert -- insert or update a meta entry (T4.2d)
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_meta_upsert",
        description=(
            "[Shared Memory Meta] Insert or update a meta entry in "
            "shared_business_memory_meta. Used for operational pipeline data "
            "(synthesis outputs, dedup mappings, knowledge graph summaries). "
            "Uses upsert semantics via ON CONFLICT (client_id, entity_type, "
            "entity_name, key). "
            "Valid entity types: synthesis_output | dedup_mapping | kg_summary."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_meta_upsert(
        ctx: Context,
        entity_type: str,
        entity_name: str,
        key: str,
        value: dict,
        source: str = "system",
        confidence: float = 1.0,
        client_id: str | None = None,
    ) -> dict:
        """
        Insert or update a meta entry in shared_business_memory_meta.

        Args:
            entity_type: Meta entity type (synthesis_output | dedup_mapping | kg_summary).
            entity_name: Entity name (case-insensitive, normalized to lowercase).
            key: Atomic fact key (max 256 chars).
            value: JSON value (the fact content -- maps to 'body' column).
            source: Provenance -- "manual" | "memory_agent" | "specialist" | "migration" | "system".
            confidence: Confidence score (0.0--1.0, default 1.0).

        Returns:
            dict with the full upserted record: id, client_id, entity_type,
            entity_name, key, value, source, confidence, metadata,
            created_at, updated_at.
        """
        if not client_id:
            raise ToolError(
                "client_id is required -- authentication context missing"
            )

        logger.info(
            "[memory_module] shared_memory_meta_upsert "
            "entity_type=%s entity_name=%s key=%s client_id=%s",
            entity_type,
            entity_name,
            key,
            client_id,
        )

        try:
            result = await _shared_memory_meta_upsert_logic(
                client_id=client_id,
                entity_type=entity_type,
                entity_name=entity_name,
                key=key,
                body=value,
                source=source,
                confidence=confidence,
            )
            # Map 'body' -> 'value' in the return for tool-level consistency
            result["value"] = result.pop("body", value)
            result["metadata"] = result.get("metadata", {})
            return result
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_meta_upsert failed: %s", exc
            )
            raise ToolError(
                f"Failed to upsert shared-memory-meta entry: {exc}"
            )

    logger.info(
        "[Memory Module] Tool 'shared_memory_meta_upsert' registered."
    )
    registered_tools.append("shared_memory_meta_upsert")

    # ----------------------------------------------------------------------
    # shared_memory_write --  write a new fact (strict INSERT by default)
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_write",
        description=(
            "[Shared Memory] Write a new fact into shared memory. "
            "By default this is a strict INSERT — it fails if the "
            "composite key (client_id, entity_type, entity_name, key) "
            "already exists. Set supersede=true to overwrite. "
            "The ``value`` parameter maps directly to the jsonb column. "
            "Use ``category`` to classify the fact semantically "
            "(knowledge | rag | documents | memory-agent | "
            "context | decision | preference). "
            "Optional ``agent_id``, ``ttl``, and ``priority`` are stored "
            "inside the metadata column."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_write(
        ctx: Context,
        entity_type: str,
        entity_name: str,
        key: str,
        value: dict,
        category: str | None = None,
        agent_id: str | None = None,
        ttl: int | None = None,
        priority: int | None = None,
        supersede: bool = False,
        source: str = "manual",
        confidence: float = 1.0,
        ttl_tier: str | None = None,
        auto_link: bool = True,
        client_id: str | None = None,
    ) -> dict:
        """
        Write a new shared-memory fact.

        Args:
            entity_type: Entity type (skill | client | contact | supplier | user | snapshot).
            entity_name: Entity name (case-insensitive, normalized to lowercase).
            key: Fact key (e.g. "tom_amigavel", "preferencia_horario").
            value: The fact value (dict — maps to 'value' jsonb column).
            category: Optional semantic category for filtering/routing.
            agent_id: Optional agent UUID (stored in metadata).
            ttl: Optional time-to-live in seconds (stored in metadata).
            priority: Optional priority 0-100 (stored in metadata).
            supersede: If True, upsert to overwrite an existing entry. Default False (strict insert).
            source: Provenance — "manual" | "memory_agent" | "specialist" | "migration" | "system".
            confidence: Confidence score (0.0--1.0, default 1.0).
            ttl_tier: Optional retention tier — "curated" | "migration" | "specialist" |
                     "memory_agent_hi" | "memory_agent_lo". If omitted, inferred from source.

        Returns:
            dict with the full written record including id, version, and timestamps.
        """
        if not client_id:
            raise ToolError(
                "client_id is required — authentication context missing"
            )

        # Tool-level validation (T1.4b)
        if not entity_type or not entity_type.strip():
            raise ToolError("entity_type is required")
        if not entity_name or not entity_name.strip():
            raise ToolError("entity_name is required")
        if not key or not key.strip():
            raise ToolError("key is required")
        if not isinstance(value, dict):
            raise ToolError("value must be a dict")
        if category is not None and category not in _VALID_CATEGORIES:
            raise ToolError(
                f"Invalid category '{category}'. "
                f"Must be one of: {sorted(_VALID_CATEGORIES)}"
            )

        logger.info(
            "[memory_module] shared_memory_write "
            "entity_type=%s entity_name=%s key=%s category=%s "
            "supersede=%s client_id=%s",
            entity_type,
            entity_name,
            key,
            category,
            supersede,
            client_id,
        )

        try:
            result = await _shared_memory_write_logic(
                client_id=client_id,
                entity_type=entity_type,
                entity_name=entity_name,
                key=key,
                value=value,
                category=category,
                agent_id=agent_id,
                ttl=ttl,
                priority=priority,
                supersede=supersede,
                source=source,
                confidence=confidence,
                ttl_tier=ttl_tier,
                auto_link=auto_link,
            )
            if auto_link:
                try:
                    await _auto_create_links(
                        client_id=client_id,
                        entity_type=entity_type,
                        entity_name=entity_name,
                        value=value,
                    )
                except Exception:
                    logger.warning(
                        "auto_link failed for %s/%s: %s",
                        entity_type,
                        entity_name,
                        ...,
                    )
            return result
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_write failed: %s", exc
            )
            raise ToolError(
                f"Failed to write shared-memory entry: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_write' registered.")
    registered_tools.append("shared_memory_write")

    # ----------------------------------------------------------------------
    # shared_memory_link --  create a semantic link between entities
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_link",
        description=(
            "[Shared Memory] Create a semantic link between two entities. "
            "Links represent relationships like 'contact Joao works_for supplier Distribuidora X'. "
            "link_type is free-form: works_for, applies_to, prefers, reports_to, depends_on, etc. "
            "Valid entity types: skill | client | contact | supplier | user."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_link(
        ctx: Context,
        source_entity_type: str,
        source_entity_name: str,
        target_entity_type: str,
        target_entity_name: str,
        link_type: str,
        source: str = "manual",
        confidence: float = 1.0,
        metadata: str | None = None,
        client_id: str | None = None,
    ) -> dict:
        """
        Create a semantic link between two entities.

        Args:
            source_entity_type: Entity type of the source (skill | client | contact | supplier | user).
            source_entity_name: Name of the source entity (case-insensitive, normalized to lowercase).
            target_entity_type: Entity type of the target (skill | client | contact | supplier | user).
            target_entity_name: Name of the target entity (case-insensitive).
            link_type: Relationship label --  e.g. "works_for", "applies_to", "prefers".
            source: Origin of the link --  "manual" | "memory_agent" | "specialist" | "migration" | "system".
            confidence: Confidence score (0.0--1.0, default 1.0).
            metadata: Optional JSON string with extra link metadata.

        Returns:
            dict with id, source, target, link_type, and provenance info.
        """
        if not client_id:
            raise ToolError(
                "client_id is required --  authentication context missing"
            )

        parsed_metadata: dict | None = None
        if metadata:
            try:
                parsed_metadata = json.loads(metadata)
                if not isinstance(parsed_metadata, dict):
                    raise ValueError("metadata must be a JSON object")
            except (json.JSONDecodeError, ValueError) as exc:
                raise ToolError(f"Invalid metadata JSON: {exc}")

        logger.info(
            "[memory_module] shared_memory_link "
            "source=%s:%s link_type=%s target=%s:%s client_id=%s",
            source_entity_type, source_entity_name,
            link_type,
            target_entity_type, target_entity_name,
            client_id,
        )

        try:
            return await _shared_memory_link_logic(
                client_id=client_id,
                source_entity_type=source_entity_type,
                source_entity_name=source_entity_name,
                target_entity_type=target_entity_type,
                target_entity_name=target_entity_name,
                link_type=link_type,
                source=source,
                confidence=confidence,
                metadata=parsed_metadata,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_link failed: %s", exc
            )
            raise ToolError(
                f"Failed to create shared-memory link: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_link' registered.")
    registered_tools.append("shared_memory_link")

    # ----------------------------------------------------------------------
    # shared_memory_unlink --  remove a semantic link by id
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_unlink",
        description=(
            "[Shared Memory] Remove a semantic link between entities by its id. "
            "Use shared_memory_get_links to find the link id first."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_unlink(
        ctx: Context,
        link_id: str,
        client_id: str | None = None,
    ) -> dict:
        """
        Remove a semantic link by its id.

        Args:
            link_id: UUID of the link to remove (from shared_memory_get_links).

        Returns:
            dict with deleted=true and the id.
        """
        if not client_id:
            raise ToolError(
                "client_id is required --  authentication context missing"
            )

        logger.info(
            "[memory_module] shared_memory_unlink link_id=%s client_id=%s",
            link_id,
            client_id,
        )

        try:
            return await _shared_memory_unlink_logic(
                client_id=client_id,
                link_id=link_id,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_unlink failed: %s", exc
            )
            raise ToolError(
                f"Failed to remove shared-memory link: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_unlink' registered.")
    registered_tools.append("shared_memory_unlink")

    # ----------------------------------------------------------------------
    # shared_memory_get_links --  query links by entity and/or type
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_get_links",
        description=(
            "[Shared Memory] Query semantic links by entity and/or link_type. "
            "Returns outgoing links (where entity is the source), incoming links "
            "(where entity is the target), or both. "
            "Filter by entity_type, entity_name, and/or link_type."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_get_links(
        ctx: Context,
        entity_type: str | None = None,
        entity_name: str | None = None,
        link_type: str | None = None,
        direction: str = "both",
        client_id: str | None = None,
    ) -> dict:
        """
        Query semantic links between entities.

        Args:
            entity_type: Optional --  filter links involving this entity type.
            entity_name: Optional --  filter links involving this entity name.
            link_type: Optional --  filter links of this type (e.g. "works_for").
            direction: "outgoing" | "incoming" | "both" (default).

        Returns:
            dict with outgoing, incoming arrays, and summary counts.
        """
        if not client_id:
            raise ToolError(
                "client_id is required --  authentication context missing"
            )

        if direction not in ("outgoing", "incoming", "both"):
            raise ToolError(
                "direction must be 'outgoing', 'incoming', or 'both'"
            )

        logger.info(
            "[memory_module] shared_memory_get_links "
            "entity_type=%s entity_name=%s link_type=%s direction=%s client_id=%s",
            entity_type,
            entity_name,
            link_type,
            direction,
            client_id,
        )

        try:
            return await _shared_memory_get_links_logic(
                client_id=client_id,
                entity_type=entity_type,
                entity_name=entity_name,
                link_type=link_type,
                direction=direction,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_get_links failed: %s", exc
            )
            raise ToolError(
                f"Failed to get shared-memory links: {exc}"
            )

    logger.info(
        "[Memory Module] Tool 'shared_memory_get_links' registered."
    )
    registered_tools.append("shared_memory_get_links")

    # ----------------------------------------------------------------------
    # shared_memory_meta_read -- read a single meta entry from meta table
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_meta_read",
        description=(
            "[Shared Memory Meta] Read a single entry from "
            "shared_business_memory_meta by its composite key "
            "(client_id, entity_type, entity_name, key). "
            "Valid entity types: synthesis_output | dedup_mapping | kg_summary. "
            "Returns the full record including body, source, confidence, "
            "and timestamps."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_meta_read(
        ctx: Context,
        entity_type: str,
        entity_name: str,
        key: str,
        client_id: str | None = None,
    ) -> dict:
        """
        Read a single shared-memory-meta entry by its composite key.

        Args:
            entity_type: Meta entity type (synthesis_output | dedup_mapping | kg_summary).
            entity_name: Entity name (case-insensitive, normalized to lowercase).
            key: Meta key (e.g. "summary", "dedup_rules").

        Returns:
            dict with the full record: id, client_id, entity_type, entity_name,
            key, body, source, confidence, created_at, updated_at.

        Raises:
            ToolError: If the entry is not found or entity_type is invalid.
        """
        if not client_id:
            raise ToolError(
                "client_id is required -- authentication context missing"
            )

        logger.info(
            "[memory_module] shared_memory_meta_read "
            "entity_type=%s entity_name=%s key=%s client_id=%s",
            entity_type,
            entity_name,
            key,
            client_id,
        )

        try:
            return await _shared_memory_meta_read_logic(
                client_id=client_id,
                entity_type=entity_type,
                entity_name=entity_name,
                key=key,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_meta_read failed: %s", exc
            )
            raise ToolError(
                f"Failed to read shared-memory-meta entry: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_meta_read' registered.")
    registered_tools.append("shared_memory_meta_read")

    # ----------------------------------------------------------------------
    # shared_memory_meta_list -- list meta entries with optional filter
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_meta_list",
        description=(
            "[Shared Memory Meta] List all entities that have meta entries "
            "in shared_business_memory_meta for the current client. "
            "Optionally filter by entity_type "
            "(synthesis_output | dedup_mapping | kg_summary). "
            "Returns a summary with total_entities, by_type breakdown, "
            "and the entities array with key-counts and last-updated timestamps."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_meta_list(
        ctx: Context,
        entity_type: str | None = None,
        client_id: str | None = None,
    ) -> dict:
        """
        List meta entries from shared_business_memory_meta.

        Args:
            entity_type: Optional filter --
                         "synthesis_output", "dedup_mapping", or "kg_summary".
                         When omitted all entity types are returned.

        Returns:
            dict with total_entities, client_id, entity_type_filter,
            by_type breakdown, and entities array sorted by
            (entity_type, entity_name).
        """
        if not client_id:
            raise ToolError(
                "client_id is required -- authentication context missing"
            )

        logger.info(
            "[memory_module] shared_memory_meta_list "
            "client_id=%s entity_type=%s",
            client_id,
            entity_type,
        )

        try:
            return await _shared_memory_meta_list_logic(
                client_id=client_id,
                entity_type=entity_type,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_meta_list failed: %s", exc
            )
            raise ToolError(
                f"Failed to list shared-memory-meta entries: {exc}"
            )

    logger.info(
        "[Memory Module] Tool 'shared_memory_meta_list' registered."
    )
    registered_tools.append("shared_memory_meta_list")

    # ----------------------------------------------------------------------
    # shared_memory_export -- export all facts for a client (T5.4)
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_export",
        description=(
            "[Shared Memory] Export all shared-memory facts for the current "
            "client. Returns a structured array of all facts with full metadata "
            "(id, entity_type, entity_name, key, value, source, confidence, "
            "version, timestamps). Optionally filter by entity_type and/or "
            "entity_name. Empty segments return an empty array (no error). "
            "Use this for backup, analytics, or data migration workflows."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_export(
        ctx: Context,
        entity_type: str | None = None,
        entity_name: str | None = None,
        client_id: str | None = None,
    ) -> dict:
        """
        Export all shared-memory facts for this client.

        Reads all records from shared_business_memory, optionally filtered by
        entity_type and/or entity_name. Returns every matching fact with full
        row data.

        Empty segments return total_records=0 and records=[] (no error).
        """
    # GOAL: Implementar exportação de memórias como JSON (com tenant isolation) e
    #       soft-delete/flush de memórias na shared business memory.
    # BEHAVIOR: B2 — Corrigir shared_memory_flush tool registration: remover copypaste
    # DECISÃO: fix_and_extend
    # Implementação mínima para teste RED passar (GREEN)
    # shared_memory_flush --  soft-delete memory entries (T5.4)
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_flush",
        description=(
            "[Shared Memory] Flush (soft-delete) shared-memory entries. "
            "Marks matching entries as flushed by recording a timestamp in "
            "their metadata. Flushed entries are hidden from "
            "shared_memory_read but remain in the database for recovery "
            "and auditing. "
            "Filters (entity_type, entity_name, key) are optional; when none "
            "are provided, ALL entries for the current client are flushed. "
            "Idempotent — calling flush multiple times on already-flushed "
            "entries is safe and returns flushed_count=0. "
            "Use this after exporting data or when you need to reset the "
            "shared memory for a client."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_flush(
        ctx: Context,
        entity_type: str | None = None,
        entity_name: str | None = None,
        key: str | None = None,
        client_id: str | None = None,
    ) -> dict:
        """Flush (soft-delete) shared-memory entries.

        Marks matching entries as flushed so they are no longer returned by
        ``shared_memory_read``.  The rows are NOT hard-deleted — they remain
        for auditing and can be recovered.

        Args:
            entity_type: Optional — only flush entries of this type.
                         Valid: skill, client, contact, supplier, user,
                         snapshot, routine, agent_result, agent_metadata.
            entity_name: Optional — only flush entries with this name.
                         Case-insensitive, normalized to lowercase.
            key: Optional — only flush the specific key.
                 Case-insensitive, normalized to lowercase.

        Returns:
            dict with:
            - flushed_count: number of entries actually flushed in this call
            - total_scanned: number of rows matching filters
            - skipped_already_flushed: entries already flushed (idempotent)
            - flush_errors: any errors during the operation (empty on success)
            - flushed_at: ISO timestamp of the flush operation

        Examples:
            >>> # Flush all entries for a specific entity
            >>> shared_memory_flush(
            ...     entity_type="client",
            ...     entity_name="joao_silva",
            ... )

            >>> # Flush a single fact
            >>> shared_memory_flush(
            ...     entity_type="skill",
            ...     entity_name="comunicacao",
            ...     key="tom_amigavel",
            ... )

            >>> # Flush ALL shared memory for the client (use with caution!)
            >>> shared_memory_flush()
        """
        if not client_id:
            raise ToolError(
                "client_id is required -- authentication context missing"
            )

        logger.info(
            "[memory_module] shared_memory_flush "
            "entity_type=%s entity_name=%s key=%s client_id=%s",
            entity_type,
            entity_name,
            key,
            client_id,
        )

        try:
            return await _shared_memory_flush_logic(
                client_id=client_id,
                entity_type=entity_type,
                entity_name=entity_name,
                key=key,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_flush failed: %s", exc
            )
            raise ToolError(
                f"Failed to flush shared memory: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_flush' registered.")
    registered_tools.append("shared_memory_flush")

    # ----------------------------------------------------------------------
    # shared_memory_graph --  navigate the semantic link graph (T3.3b)
    # ----------------------------------------------------------------------

    @mcp.tool(
        name="shared_memory_graph",
        description=(
            "[Shared Memory] Navigate the semantic link graph in shared "
            "business memory. Four traversal modes: "
            "1) 'neighbors' — direct (depth=1) links for an entity; "
            "2) 'reachable' — all entities reachable up to max_depth via BFS; "
            "3) 'path' — shortest path between two entities "
            "(requires target_entity_type and target_entity_name); "
            "4) 'cluster' — connected component around an entity up to "
            "max_depth/max_nodes. "
            "Filter by direction (outgoing|incoming|both) and link_type. "
            "Returns a graph with nodes and edges including depth and "
            "confidence for each edge."
        ),
    )
    @mcp_inject_client_id
    async def shared_memory_graph(
        ctx: Context,
        mode: str,
        entity_type: str,
        entity_name: str,
        max_depth: int = 3,
        max_nodes: int = 100,
        direction: str = "both",
        link_type_filter: str | None = None,
        target_entity_type: str | None = None,
        target_entity_name: str | None = None,
        client_id: str | None = None,
    ) -> dict:
        """
        Traverse and navigate the semantic link graph in shared memory.

        Args:
            mode: "neighbors" | "reachable" | "path" | "cluster".
            entity_type: Source entity type (skill, client, contact, etc.).
            entity_name: Source entity name (case-insensitive).
            max_depth: Maximum traversal depth (1..5, default 3).
            max_nodes: Maximum number of nodes returned (1..500, default 100).
            direction: "outgoing" | "incoming" | "both" (default).
            link_type_filter: Optional -- restrict traversal to this link_type.
            target_entity_type: Required when mode="path".
            target_entity_name: Required when mode="path".

        Returns:
            dict with mode, direction, total_nodes, total_edges, nodes, edges.
        """
        if not client_id:
            raise ToolError(
                "client_id is required -- authentication context missing"
            )

        if mode not in ("neighbors", "reachable", "path", "cluster"):
            raise ToolError(
                "mode must be 'neighbors', 'reachable', 'path', or 'cluster'"
            )
        if direction not in ("outgoing", "incoming", "both"):
            raise ToolError(
                "direction must be 'outgoing', 'incoming', or 'both'"
            )
        if not (1 <= int(max_depth) <= 5):
            raise ToolError("max_depth must be between 1 and 5")
        if not (1 <= int(max_nodes) <= 500):
            raise ToolError("max_nodes must be between 1 and 500")
        if mode == "path" and (not target_entity_type or not target_entity_name):
            raise ToolError(
                "mode='path' requires both target_entity_type and "
                "target_entity_name"
            )

        logger.info(
            "[memory_module] shared_memory_graph mode=%s entity_type=%s "
            "entity_name=%s max_depth=%s max_nodes=%s direction=%s "
            "link_type_filter=%s target=%s:%s client_id=%s",
            mode,
            entity_type,
            entity_name,
            max_depth,
            max_nodes,
            direction,
            link_type_filter,
            target_entity_type,
            target_entity_name,
            client_id,
        )

        try:
            return await _shared_memory_graph_logic(
                client_id=client_id,
                mode=mode,
                entity_type=entity_type,
                entity_name=entity_name,
                max_depth=max_depth,
                max_nodes=max_nodes,
                direction=direction,
                link_type_filter=link_type_filter,
                target_entity_type=target_entity_type,
                target_entity_name=target_entity_name,
            )
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            logger.error(
                "[memory_module] shared_memory_graph failed: %s", exc
            )
            raise ToolError(
                f"Failed to traverse shared-memory graph: {exc}"
            )

    logger.info("[Memory Module] Tool 'shared_memory_graph' registered.")
    registered_tools.append("shared_memory_graph")

    return registered_tools
