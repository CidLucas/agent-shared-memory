"""
Entity normalization & validation helpers for shared_business_memory.

Single source of truth for:
  - VALID_ENTITY_TYPES          (canonical frozenset of 9 entity types)
  - validate_entity_type()      (raises ValueError for unknown types)
  - normalize_entity_name()     (lightweight: lowercase + strip — used for
                                 matching/lookup keys in shared_business_memory)
  - normalize_entity_name_strict() (heavyweight: NFKD + strip accents +
                                 whitespace -> underscore + remove punctuation +
                                 optional contact: prefix — used as canonical
                                 LightRAG entity IDs in sbm_to_lightrag_synthesis)

Replaces four near-identical copies that lived in:
  - tool_modules/memory_module.py
  - tool_modules/memory_post_flight.py
  - tool_modules/version_module.py
  - tool_modules/sbm_to_lightrag_synthesis.py
"""

from __future__ import annotations

import re
import unicodedata


VALID_ENTITY_TYPES: frozenset[str] = frozenset(
    {
        "skill",
        "client",
        "contact",
        "supplier",
        "user",
        "snapshot",
        "routine",
        "agent_result",
        "agent_metadata",
    }
)


def validate_entity_type(
    entity_type: str,
    field_name: str = "entity_type",
) -> None:
    """
    Raise ``ValueError`` if ``entity_type`` is not a known entity type.

    Args:
        entity_type: The entity type string to validate.
        field_name: Field name to surface in the error message (e.g. when
                    validating ``source_entity_type`` vs ``target_entity_type``).
    """
    if entity_type not in VALID_ENTITY_TYPES:
        raise ValueError(
            f"Invalid {field_name} '{entity_type}'. "
            f"Must be one of: {sorted(VALID_ENTITY_TYPES)}"
        )


def normalize_entity_name(name: str) -> str:
    """
    Lightweight entity name normalization: strip + lowercase.

    Used for matching/lookup keys in shared_business_memory where the
    existing data was stored with this format. Behaviour preserved
    verbatim from the pre-refactor copies in memory_module,
    memory_post_flight, and version_module.
    """
    return name.strip().lower()


# Whitespace runs -> single underscore (used by strict variant).
_WS_RE = re.compile(r"\s+")
# Punctuation that should be removed in the strict variant
# (keep letters/digits/underscore; everything else is dropped).
_PUNCT_RE = re.compile(r"[^\w]+", re.UNICODE)
# Collapse runs of underscores that may result from punctuation removal.
_MULTI_UNDERSCORE_RE = re.compile(r"_+")


def normalize_entity_name_strict(
    name: str,
    entity_type: str | None = None,
) -> str:
    """
    Strict canonicalization for LightRAG entity IDs (sbm_to_lightrag_synthesis).

    Steps (DD-T41-07):
      1. NFKD decomposition + strip combining marks (remove accents).
      2. Lowercase.
      3. Whitespace runs -> single underscore.
      4. Remove remaining punctuation.
      5. For ``entity_type='contact'``, prefix with ``contact:``
         (R2 mitigation against collisions with skill names).

    Behaviour preserved verbatim from the pre-refactor copy in
    sbm_to_lightrag_synthesis.py::normalize_entity_name.

    Args:
        name: Raw entity name (e.g. "João da Silva").
        entity_type: Optional entity_type hint. When ``"contact"``, the
                     result is prefixed with ``"contact:"``.

    Returns:
        Canonical ID string, e.g. ``"joao_da_silva"`` or ``"contact:joao_da_silva"``.
    """
    if not name:
        return ""

    decomposed = unicodedata.normalize("NFKD", name)
    ascii_name = decomposed.encode("ascii", "ignore").decode("ascii")
    normalized = ascii_name.lower()
    normalized = _WS_RE.sub("_", normalized.strip())
    normalized = _PUNCT_RE.sub("", normalized)
    normalized = _MULTI_UNDERSCORE_RE.sub("_", normalized).strip("_")
    if entity_type == "contact" and normalized and not normalized.startswith("contact:"):
        normalized = f"contact:{normalized}"
    return normalized
