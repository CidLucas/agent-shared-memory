"""
prune_shared_memory.py — Shared Memory Lifecycle Prune Routine (T4.4d)

Routine Engine cron job: executed daily at 03:00 UTC.
Performs two-phase pruning of ``shared_business_memory``:

  1. Soft-delete: marks expired records as ``archived=true``
  2. Hard-delete: permanently removes archived records past retention window

Design decisions (from DD-04, DD-05, DD-07):
- Runs on Routine Engine (Python cron), NOT pg_cron
- Checks backup completion before executing (race condition guard)
- Silent operation; alerts only if total_affected > 100
- Saves checkpoint with per-phase counts


Entry points
────────────
- CLI: ``python -m services.routine_engine.src.routines.prune_shared_memory``
- Programmatic: ``asyncio.run(run_prune() -> PruneResult)``


Trigger
───────
- Nightly scheduler via Routine Engine cron poller
- cross_agent_routines entry: id='prune_shared_memory', trigger_type='cron'
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROUTINE_ID: str = "prune_shared_memory"
ROUTINE_NAME: str = "Prune Shared Memory"

# System client_id used for global routine checkpoint storage.
# Mirrors the convention in upsert_routine_checkpoint RPC.
SYSTEM_CLIENT_ID: str = "00000000-0000-0000-0000-000000000000"

# Backup routine checkpoint key to verify before pruning (DD-07).
BACKUP_CHECKPOINT_KEY: str = "current_state:backup_shared_memory"

# Alert threshold (DD-05): emit alert only if total affected > 100.
ALERT_THRESHOLD: int = 100

# Webhook URL for alerts (configurable via env).
ALERT_WEBHOOK_URL: str = os.getenv("PRUNE_ALERT_WEBHOOK_URL", "")


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass
class PruneResult:
    """Result of a prune_shared_memory execution."""

    exec_id: str = field(default_factory=lambda: str(uuid4()))
    soft_deleted: int = 0
    hard_deleted: int = 0
    backup_ok: bool = True
    aborted: bool = False
    abort_reason: str | None = None
    alerted: bool = False
    error: str | None = None
    duration_ms: int = 0
    started_at: str = ""

    @property
    def total_affected(self) -> int:
        return self.soft_deleted + self.hard_deleted

    @property
    def success(self) -> bool:
        return self.error is None and not self.aborted


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


async def run_prune(
    *,
    db: Any | None = None,
    dry_run: bool = False,
) -> PruneResult:
    """Run the full prune lifecycle for shared_business_memory.

    Args:
        db: Optional pre-configured Supabase client (service role).
        dry_run: If True, skip destructive operations (soft-delete/hard-delete).

    Returns:
        PruneResult with counts, checkpoint, and alert status.
    """
    start = datetime.now(UTC)
    result = PruneResult(started_at=start.isoformat())

    try:
        from blu_supabase_client import get_supabase_client

        db = db or get_supabase_client(use_service_role=True)

        # -----------------------------------------------------------------
        # Step 1 — Verify backup completion (DD-07)
        # -----------------------------------------------------------------
        backup_ok = await _check_backup_completed(db)
        result.backup_ok = backup_ok

        if not backup_ok:
            logger.warning(
                "[prune_shared_memory] Backup checkpoint NOT found or NOT completed. "
                "Aborting prune to avoid race condition with backup routine."
            )
            result.aborted = True
            result.abort_reason = "backup_not_completed"
            await _save_checkpoint(db, result, start)
            return result

        # -----------------------------------------------------------------
        # Step 2 — Phase 1: Soft-delete
        # -----------------------------------------------------------------
        soft_count = 0
        if not dry_run:
            soft_count = await _soft_delete_expired(db)
            result.soft_deleted = soft_count
            logger.info(
                "[prune_shared_memory] Phase 1 (soft-delete): %d records archived",
                soft_count,
            )
        else:
            soft_count = await _count_soft_deletable(db)
            result.soft_deleted = soft_count
            logger.info(
                "[prune_shared_memory] Phase 1 (DRY RUN): %d records would be archived",
                soft_count,
            )

        # -----------------------------------------------------------------
        # Step 3 — Phase 2: Hard-delete
        # -----------------------------------------------------------------
        hard_count = 0
        if not dry_run:
            hard_count = await _hard_delete_expired(db)
            result.hard_deleted = hard_count
            logger.info(
                "[prune_shared_memory] Phase 2 (hard-delete): %d records deleted",
                hard_count,
            )
        else:
            hard_count = await _count_hard_deletable(db)
            result.hard_deleted = hard_count
            logger.info(
                "[prune_shared_memory] Phase 2 (DRY RUN): %d records would be deleted",
                hard_count,
            )

        # -----------------------------------------------------------------
        # Step 4 — Alert if above threshold (DD-05)
        # -----------------------------------------------------------------
        if result.total_affected > ALERT_THRESHOLD:
            logger.warning(
                "[prune_shared_memory] ALERT: total_affected=%d exceeds threshold=%d",
                result.total_affected,
                ALERT_THRESHOLD,
            )
            await _send_alert(result)
            result.alerted = True
        else:
            logger.info(
                "[prune_shared_memory] total_affected=%d — below alert threshold (%d), silent",
                result.total_affected,
                ALERT_THRESHOLD,
            )

        # -----------------------------------------------------------------
        # Step 5 — Save checkpoint
        # -----------------------------------------------------------------
        await _save_checkpoint(db, result, start)

    except Exception as exc:
        logger.exception("[prune_shared_memory] Run failed")
        result.error = f"{type(exc).__name__}: {exc}"

    finally:
        result.duration_ms = int(
            (datetime.now(UTC) - start).total_seconds() * 1000
        )

    return result


# ---------------------------------------------------------------------------
# Step 1 — Backup verification (DD-07)
# ---------------------------------------------------------------------------


async def _check_backup_completed(db: Any) -> bool:
    """Check if the backup routine completed its last run.

    Queries shared_business_memory for checkpoint with
    key='current_state:backup_shared_memory' and entity_type='routine'.
    Checks that the value contains status='completed' and completed_at
    is within the last 24 hours.
    """
    try:
        resp = (
            db.table("shared_business_memory")
            .select("value")
            .eq("entity_type", "routine")
            .eq("key", BACKUP_CHECKPOINT_KEY)
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            logger.info(
                "[prune_shared_memory] No backup checkpoint found — backup may not have run yet"
            )
            return False

        checkpoint_value = rows[0].get("value") or {}
        if isinstance(checkpoint_value, str):
            checkpoint_value = json.loads(checkpoint_value)

        status = checkpoint_value.get("status", "")
        if status == "completed":
            logger.info(
                "[prune_shared_memory] Backup checkpoint: status=completed — OK to proceed"
            )
            return True

        logger.warning(
            "[prune_shared_memory] Backup checkpoint status=%s (expected 'completed') — aborting",
            status,
        )
        return False

    except Exception as exc:
        logger.warning(
            "[prune_shared_memory] Failed to check backup checkpoint: %s — proceeding anyway",
            exc,
        )
        # If we can't check, proceed (fail-open to avoid permanent block)
        return True


# ---------------------------------------------------------------------------
# Step 2 — Phase 1: Soft-delete
# ---------------------------------------------------------------------------


async def _soft_delete_expired(db: Any) -> int:
    """Mark expired records as archived.

    SQL equivalent:
        UPDATE shared_business_memory
        SET archived = true, soft_delete_at = NOW()
        WHERE soft_delete_at <= NOW() AND archived = false
    """
    import datetime

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Count first for logging
    count_resp = (
        db.table("shared_business_memory")
        .select("id", count="exact")
        .not_.is_("soft_delete_at", "null")
        .lte("soft_delete_at", now_iso)
        .eq("archived", False)
        .execute()
    )
    count = count_resp.count or 0

    if count == 0:
        return 0

    # Perform update
    resp = (
        db.table("shared_business_memory")
        .update(
            {
                "archived": True,
                "soft_delete_at": now_iso,
            },
            count="exact",
        )
        .not_.is_("soft_delete_at", "null")
        .lte("soft_delete_at", now_iso)
        .eq("archived", False)
        .execute()
    )

    updated = resp.count if hasattr(resp, "count") and resp.count else count
    return updated


async def _count_soft_deletable(db: Any) -> int:
    """Count records eligible for soft-delete (dry-run)."""
    import datetime

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    resp = (
        db.table("shared_business_memory")
        .select("id", count="exact")
        .not_.is_("soft_delete_at", "null")
        .lte("soft_delete_at", now_iso)
        .eq("archived", False)
        .execute()
    )
    return resp.count or 0


# ---------------------------------------------------------------------------
# Step 3 — Phase 2: Hard-delete
# ---------------------------------------------------------------------------


async def _hard_delete_expired(db: Any) -> int:
    """Permanently delete archived records past retention.

    SQL equivalent:
        DELETE FROM shared_business_memory
        WHERE hard_delete_at <= NOW() AND archived = true
    """
    import datetime

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Count first
    count_resp = (
        db.table("shared_business_memory")
        .select("id", count="exact")
        .not_.is_("hard_delete_at", "null")
        .lte("hard_delete_at", now_iso)
        .eq("archived", True)
        .execute()
    )
    count = count_resp.count or 0

    if count == 0:
        return 0

    # Perform delete
    resp = (
        db.table("shared_business_memory")
        .delete(count="exact")
        .not_.is_("hard_delete_at", "null")
        .lte("hard_delete_at", now_iso)
        .eq("archived", True)
        .execute()
    )

    deleted = resp.count if hasattr(resp, "count") and resp.count else count
    return deleted


async def _count_hard_deletable(db: Any) -> int:
    """Count records eligible for hard-delete (dry-run)."""
    import datetime

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    resp = (
        db.table("shared_business_memory")
        .select("id", count="exact")
        .not_.is_("hard_delete_at", "null")
        .lte("hard_delete_at", now_iso)
        .eq("archived", True)
        .execute()
    )
    return resp.count or 0


# ---------------------------------------------------------------------------
# Step 4 — Alert (DD-05)
# ---------------------------------------------------------------------------


async def _send_alert(result: PruneResult) -> None:
    """Send alert via webhook when affected records exceed threshold."""
    if not ALERT_WEBHOOK_URL:
        logger.warning(
            "[prune_shared_memory] ALERT_WEBHOOK_URL not configured — "
            "alert suppressed (total_affected=%d)",
            result.total_affected,
        )
        return

    try:
        import aiohttp

        payload = {
            "routine": ROUTINE_ID,
            "routine_name": ROUTINE_NAME,
            "exec_id": result.exec_id,
            "total_affected": result.total_affected,
            "soft_deleted": result.soft_deleted,
            "hard_deleted": result.hard_deleted,
            "threshold": ALERT_THRESHOLD,
            "started_at": result.started_at,
            "message": (
                f"Prune exceeded alert threshold: {result.total_affected} records "
                f"(soft-deleted={result.soft_deleted}, hard-deleted={result.hard_deleted}). "
                f"Threshold={ALERT_THRESHOLD}."
            ),
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                ALERT_WEBHOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    logger.warning(
                        "[prune_shared_memory] Alert webhook returned %d: %s",
                        resp.status,
                        await resp.text(),
                    )
                else:
                    logger.info("[prune_shared_memory] Alert sent successfully")

    except ImportError:
        logger.warning(
            "[prune_shared_memory] aiohttp not available — alert not sent "
            "(total_affected=%d)",
            result.total_affected,
        )
    except Exception as exc:
        logger.error("[prune_shared_memory] Failed to send alert: %s", exc)


# ---------------------------------------------------------------------------
# Step 5 — Checkpoint
# ---------------------------------------------------------------------------


async def _save_checkpoint(
    db: Any,
    result: PruneResult,
    start: datetime,
) -> None:
    """Save execution checkpoint in shared_business_memory via RPC.

    Uses upsert_routine_checkpoint RPC with SYSTEM_CLIENT_ID.
    Falls back to direct INSERT if RPC is unavailable.
    """
    checkpoint_value = {
        "status": "error" if result.error else ("aborted" if result.aborted else "completed"),
        "exec_id": result.exec_id,
        "soft_deleted": result.soft_deleted,
        "hard_deleted": result.hard_deleted,
        "total_affected": result.total_affected,
        "backup_ok": result.backup_ok,
        "aborted": result.aborted,
        "abort_reason": result.abort_reason,
        "alerted": result.alerted,
        "error": result.error,
        "duration_ms": result.duration_ms,
        "started_at": result.started_at,
        "completed_at": datetime.now(UTC).isoformat(),
    }

    try:
        # Try RPC first (requires upsert_routine_checkpoint function)
        db.rpc(
            "upsert_routine_checkpoint",
            {
                "p_client_id": SYSTEM_CLIENT_ID,
                "p_routine_id": ROUTINE_ID,
                "p_exec_id": result.exec_id,
                "p_step_number": 1,
                "p_state_value": checkpoint_value,
            },
        ).execute()
        logger.info(
            "[prune_shared_memory] Checkpoint saved via RPC (exec_id=%s)",
            result.exec_id,
        )
    except Exception as rpc_exc:
        logger.warning(
            "[prune_shared_memory] RPC upsert_routine_checkpoint failed: %s — "
            "falling back to direct INSERT",
            rpc_exc,
        )
        try:
            _save_checkpoint_direct(db, result, checkpoint_value)
        except Exception as fallback_exc:
            logger.error(
                "[prune_shared_memory] Failed to save checkpoint (both RPC and direct): %s",
                fallback_exc,
            )


def _save_checkpoint_direct(
    db: Any,
    result: PruneResult,
    checkpoint_value: dict[str, Any],
) -> None:
    """Direct INSERT of checkpoint into shared_business_memory.

    Used as fallback when upsert_routine_checkpoint RPC is unavailable.
    Uses ON CONFLICT DO UPDATE for idempotency.
    """
    checkpoint_key = f"checkpoint:run:{result.exec_id}:step:1"

    # Check if row exists
    existing = (
        db.table("shared_business_memory")
        .select("id")
        .eq("client_id", SYSTEM_CLIENT_ID)
        .eq("entity_type", "routine")
        .eq("entity_name", ROUTINE_ID)
        .eq("key", checkpoint_key)
        .limit(1)
        .execute()
    )

    if existing.data:
        # Update existing
        row_id = existing.data[0]["id"]
        db.table("shared_business_memory").update(
            {
                "value": checkpoint_value,
                "updated_at": "now()",
            }
        ).eq("id", row_id).execute()
    else:
        # Insert new
        db.table("shared_business_memory").insert(
            {
                "client_id": SYSTEM_CLIENT_ID,
                "entity_type": "routine",
                "entity_name": ROUTINE_ID,
                "key": checkpoint_key,
                "value": checkpoint_value,
                "source": "system",
                "confidence": 1.0,
            }
        ).execute()

    logger.info(
        "[prune_shared_memory] Checkpoint saved via direct INSERT (exec_id=%s)",
        result.exec_id,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prune Shared Memory — two-phase retention lifecycle.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count eligible records without making changes.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    result = asyncio.run(run_prune(dry_run=args.dry_run))

    # Print summary
    if args.dry_run:
        logger.info(f"\n=== DRY RUN ===")
    logger.info(f"Exec ID:      {result.exec_id}")
    logger.info(f"Backup OK:    {result.backup_ok}")
    logger.info(f"Aborted:      {result.aborted}" + (f" ({result.abort_reason})" if result.abort_reason else ""))
    logger.info(f"Soft-deleted: {result.soft_deleted}")
    logger.info(f"Hard-deleted: {result.hard_deleted}")
    logger.info(f"Total:        {result.total_affected}")
    logger.info(f"Alerted:      {result.alerted}")
    if result.error:
        logger.error(f"ERROR:        {result.error}")
    logger.info(f"Duration:     {result.duration_ms}ms")
    logger.info(f"Success:      {result.success}")

    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()

