"""Stub para blu_context_service.context_schemas."""
from __future__ import annotations

_SNAPSHOT_DIMENSION_FIELDS: frozenset[str] = frozenset({
    "snapshot_id", "dimensao", "periodo", "gerado_em",
    "vigencia_inicio", "vigencia_fim", "indicadores", "alertas",
    "resumo_executivo",
})
