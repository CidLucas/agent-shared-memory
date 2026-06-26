"""
backup_shared_memory.py — Shared Memory Backup Routine (T5.5)

Routine Engine cron job: executed daily at 02:00 UTC.
Performs logical dump of ``shared_business_memory`` table, compresses
with gzip, uploads to Supabase Storage bucket, and writes a checkpoint
for coordination with the prune routine (T4.4d).

Design decisions (from DD-5 through DD-10):
- Runs on Routine Engine (Python cron), NOT pg_cron
- Logical dump via Supabase REST API (pg_dump not available on managed DB)
- Upload to ``shared-memory-backups/YYYY-MM-DD/dump.json.gz``
- Checkpoint in shared_business_memory (entity_type='routine')
- Buffer 1h before prune (02:00 backup → 03:00 prune)
- Retention: 30 days daily + 12 weekly consolidated

Entry points
────────────
- CLI: ``python -m services.routine_engine.src.routines.backup_shared_memory``
- Programmatic: ``asyncio.run(run_backup() -> BackupResult)``

Trigger
───────
- Nightly scheduler via Routine Engine cron poller
- cross_agent_routines entry: id='backup_shared_memory', trigger_type='cron'
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROUTINE_ID: str = "backup_shared_memory"
ROUTINE_NAME: str = "Backup Shared Memory"

# System client_id used for global routine checkpoint storage.
SYSTEM_CLIENT_ID: str = "00000000-0000-0000-0000-000000000000"

# Supabase Storage bucket name for backups.
BACKUP_BUCKET: str = "shared-memory-backups"

# Checkpoint key written to shared_business_memory for prune coordination.
CHECKPOINT_KEY: str = "current_state:backup_shared_memory"

# Retention window: daily backups beyond this many days are pruned.
DAILY_RETENTION_DAYS: int = 30

# Weekly consolidated retention: 12 weeks.
WEEKLY_RETENTION_WEEKS: int = 12

# Upload retry settings.
UPLOAD_MAX_RETRIES: int = 3
UPLOAD_RETRY_DELAY_SECONDS: float = 30.0

# Routine timeout (10 minutes).
ROUTINE_TIMEOUT_SECONDS: int = 600

# Alert webhook URL (configurable via env).
ALERT_WEBHOOK_URL: str = os.getenv("BACKUP_ALERT_WEBHOOK_URL", "")


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass
class BackupResult:
    """Result of a backup_shared_memory execution."""

    exec_id: str = field(default_factory=lambda: str(uuid4()))
    status: str = "success"  # "success" | "failed" | "timeout"
    row_count: int = 0
    size_bytes: int = 0
    sha256: str = ""
    dump_path: str = ""
    pruned_daily: int = 0
    consolidated_weekly: int = 0
    error: str | None = None
    alerts: list[str] = field(default_factory=list)
    duration_ms: int = 0
    started_at: str = ""

    @property
    def success(self) -> bool:
        return self.status == "success" and self.error is None


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


async def run_backup(
    *,
    db: Any | None = None,
    dry_run: bool = False,
) -> BackupResult:
    """Run the full backup lifecycle for shared_business_memory.

    Steps:
        1. Dump all records from shared_business_memory via Supabase REST.
        2. Compress with gzip.
        3. Upload to shared-memory-backups/{YYYY-MM-DD}/dump.json.gz.
        4. Compute sha256 + row_count.
        5. Write checkpoint in shared_business_memory.
        6. Prune old backups (>30d daily, consolidate weeklies).

    Args:
        db: Optional pre-configured Supabase client (service role).
        dry_run: If True, skip upload and destructive operations.

    Returns:
        BackupResult with counts, hash, and status.
    """
    start = datetime.now(UTC)
    result = BackupResult(started_at=start.isoformat())

    try:
        from blu_supabase_client import get_supabase_client

        db = db or get_supabase_client(use_service_role=True)

        # -----------------------------------------------------------------
        # Step 1 — Dump all records from shared_business_memory
        # -----------------------------------------------------------------
        rows = await _dump_all_records(db)
        result.row_count = len(rows)
        logger.info(
            "[backup_shared_memory] Step 1: dumped %d records from shared_business_memory",
            result.row_count,
        )

        if result.row_count == 0:
            logger.warning(
                "[backup_shared_memory] No records found — creating empty backup"
            )

        # -----------------------------------------------------------------
        # Step 2 — Compress with gzip
        # -----------------------------------------------------------------
        dump_json = json.dumps(rows, ensure_ascii=False, default=str).encode("utf-8")
        compressed = gzip.compress(dump_json, compresslevel=6)
        result.size_bytes = len(compressed)
        logger.info(
            "[backup_shared_memory] Step 2: compressed %d bytes → %d bytes (gzip)",
            len(dump_json),
            result.size_bytes,
        )

        # -----------------------------------------------------------------
        # Step 3 — Compute sha256
        # -----------------------------------------------------------------
        result.sha256 = hashlib.sha256(compressed).hexdigest()
        logger.info(
            "[backup_shared_memory] Step 3: sha256=%s",
            result.sha256[:16] + "...",
        )

        # -----------------------------------------------------------------
        # Step 4 — Upload to Supabase Storage
        # -----------------------------------------------------------------
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        result.dump_path = f"{today}/dump.json.gz"

        if not dry_run:
            upload_success = await _upload_with_retry(db, result.dump_path, compressed)
            if not upload_success:
                result.status = "failed"
                result.error = "Upload failed after max retries"
                logger.error(
                    "[backup_shared_memory] Upload FAILED after %d retries",
                    UPLOAD_MAX_RETRIES,
                )
                await _save_checkpoint(db, result, start)
                return result
            logger.info(
                "[backup_shared_memory] Step 4: uploaded to %s/%s (%d bytes)",
                BACKUP_BUCKET,
                result.dump_path,
                result.size_bytes,
            )
        else:
            logger.info(
                "[backup_shared_memory] Step 4 (DRY RUN): would upload to %s/%s",
                BACKUP_BUCKET,
                result.dump_path,
            )

        # -----------------------------------------------------------------
        # Step 5 — Write checkpoint in shared_business_memory
        # -----------------------------------------------------------------
        if not dry_run:
            await _save_checkpoint(db, result, start)
            logger.info(
                "[backup_shared_memory] Step 5: checkpoint written (status=%s)",
                result.status,
            )

        # -----------------------------------------------------------------
        # Step 6 — Prune old backups
        # -----------------------------------------------------------------
        if not dry_run:
            result.pruned_daily = await _prune_daily_backups(db, today)
            result.consolidated_weekly = await _consolidate_weekly_backups(db, today)
            logger.info(
                "[backup_shared_memory] Step 6: pruned %d daily, consolidated %d weekly",
                result.pruned_daily,
                result.consolidated_weekly,
            )

    except asyncio.TimeoutError:
        result.status = "timeout"
        result.error = f"Routine exceeded {ROUTINE_TIMEOUT_SECONDS}s timeout"
        logger.error("[backup_shared_memory] TIMEOUT after %ds", ROUTINE_TIMEOUT_SECONDS)
        await _save_checkpoint(db, result, start)

    except Exception as exc:
        logger.exception("[backup_shared_memory] Run failed")
        result.status = "failed"
        result.error = f"{type(exc).__name__}: {exc}"
        if not dry_run:
            try:
                await _save_checkpoint(db, result, start)
            except Exception as checkpoint_exc:
                logger.error(
                    "[backup_shared_memory] Failed to save failure checkpoint: %s",
                    checkpoint_exc,
                )

    finally:
        result.duration_ms = int(
            (datetime.now(UTC) - start).total_seconds() * 1000
        )

    return result


# ---------------------------------------------------------------------------
# Step 1 — Dump all records
# ---------------------------------------------------------------------------


async def _dump_all_records(db: Any) -> list[dict[str, Any]]:
    """Fetch all records from shared_business_memory in paginated batches.

    Uses Supabase REST API with pagination (limit 1000 per page).
    """
    all_rows: list[dict[str, Any]] = []
    page_size = 1000
    offset = 0

    while True:
        resp = await asyncio.to_thread(
            lambda: db.table("shared_business_memory")
            .select("*")
            .order("id")
            .range(offset, offset + page_size - 1)
            .execute()
        )

        batch = resp.data or []
        if not batch:
            break

        all_rows.extend(batch)

        if len(batch) < page_size:
            break

        offset += page_size

    return all_rows


# ---------------------------------------------------------------------------
# Step 4 — Upload with retry
# ---------------------------------------------------------------------------


async def _upload_with_retry(
    db: Any,
    path: str,
    data: bytes,
) -> bool:
    """Upload compressed dump to Supabase Storage with retry logic.

    Returns True on success, False after exhausting retries.
    """
    last_error: str | None = None

    for attempt in range(1, UPLOAD_MAX_RETRIES + 1):
        try:
            await asyncio.to_thread(
                lambda: db.storage.from_(BACKUP_BUCKET).upload(
                    path=path,
                    file=data,
                    file_options={"content-type": "application/gzip", "upsert": "true"},
                )
            )
            return True

        except Exception as exc:
            last_error = str(exc)
            logger.warning(
                "[backup_shared_memory] Upload attempt %d/%d failed: %s",
                attempt,
                UPLOAD_MAX_RETRIES,
                last_error,
            )

            if attempt < UPLOAD_MAX_RETRIES:
                await asyncio.sleep(UPLOAD_RETRY_DELAY_SECONDS)

    logger.error(
        "[backup_shared_memory] All %d upload attempts failed. Last error: %s",
        UPLOAD_MAX_RETRIES,
        last_error,
    )
    return False


# ---------------------------------------------------------------------------
# Step 5 — Checkpoint
# ---------------------------------------------------------------------------


async def _save_checkpoint(
    db: Any,
    result: BackupResult,
    start: datetime,
) -> None:
    """Save execution checkpoint in shared_business_memory.

    Uses upsert_routine_checkpoint RPC with SYSTEM_CLIENT_ID.
    Falls back to direct INSERT if RPC is unavailable.
    """
    checkpoint_value = {
        "completed_at": datetime.now(UTC).isoformat(),
        "status": "completed" if result.status == "success" else result.status,
        "size_bytes": result.size_bytes,
        "row_count": result.row_count,
        "sha256": result.sha256,
        "dump_path": result.dump_path,
        "routine_run_id": result.exec_id,
        "error": result.error,
        "duration_ms": result.duration_ms,
        "started_at": result.started_at,
        "pruned_daily": result.pruned_daily,
        "consolidated_weekly": result.consolidated_weekly,
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
            "[backup_shared_memory] Checkpoint saved via RPC (exec_id=%s)",
            result.exec_id,
        )
    except Exception as rpc_exc:
        logger.warning(
            "[backup_shared_memory] RPC upsert_routine_checkpoint failed: %s — "
            "falling back to direct INSERT",
            rpc_exc,
        )
        try:
            _save_checkpoint_direct(db, result, ROUTINE_ID, checkpoint_value)
        except Exception as fallback_exc:
            logger.error(
                "[backup_shared_memory] Failed to save checkpoint (both RPC and direct): %s",
                fallback_exc,
            )


def _save_checkpoint_direct(
    db: Any,
    result: BackupResult,
    routine_id: str,
    checkpoint_value: dict[str, Any],
) -> None:
    """Direct UPSERT of checkpoint into shared_business_memory.

    Used as fallback when upsert_routine_checkpoint RPC is unavailable.
    """
    # Check if row exists
    existing = (
        db.table("shared_business_memory")
        .select("id")
        .eq("client_id", SYSTEM_CLIENT_ID)
        .eq("entity_type", "routine")
        .eq("entity_name", routine_id)
        .eq("key", CHECKPOINT_KEY)
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
                "entity_name": routine_id,
                "key": CHECKPOINT_KEY,
                "value": checkpoint_value,
                "source": "system",
                "confidence": 1.0,
            }
        ).execute()

    logger.info(
        "[backup_shared_memory] Checkpoint saved via direct INSERT (exec_id=%s)",
        result.exec_id,
    )


# ---------------------------------------------------------------------------
# Step 6 — Prune old daily backups
# ---------------------------------------------------------------------------


async def _prune_daily_backups(db: Any, today_str: str) -> int:
    """Remove daily backups older than DAILY_RETENTION_DAYS.

    Returns count of removed files.
    """
    cutoff_date = datetime.now(UTC) - timedelta(days=DAILY_RETENTION_DAYS)
    pruned = 0

    try:
        # List all files in the bucket
        resp = await asyncio.to_thread(
            lambda: db.storage.from_(BACKUP_BUCKET).list()
        )

        files = resp or []
        for item in files:
            name = item.get("name", "")
            # Daily backups follow pattern: YYYY-MM-DD/dump.json.gz
            if not name.endswith("/dump.json.gz"):
                continue

            date_str = name.split("/")[0]
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                    tzinfo=UTC
                )
            except ValueError:
                continue

            if file_date < cutoff_date:
                await asyncio.to_thread(
                    lambda n=name: db.storage.from_(BACKUP_BUCKET).remove([n])
                )
                pruned += 1
                logger.info(
                    "[backup_shared_memory] Pruned old daily backup: %s", name
                )

        logger.info(
            "[backup_shared_memory] Pruned %d daily backups older than %s",
            pruned,
            cutoff_date.strftime("%Y-%m-%d"),
        )

    except Exception as exc:
        logger.warning(
            "[backup_shared_memory] Daily backup prune failed (non-fatal): %s", exc
        )

    return pruned


# ---------------------------------------------------------------------------
# Step 6 — Consolidate weekly backups
# ---------------------------------------------------------------------------


async def _consolidate_weekly_backups(db: Any, today_str: str) -> int:
    """Consolidate the latest backup of each week into a weekly archive.

    For the current week, copies the daily backup to a weekly path.
    Removes weeklies older than WEEKLY_RETENTION_WEEKS.

    Returns count of consolidated files.
    """
    consolidated = 0
    today = datetime.strptime(today_str, "%Y-%m-%d").replace(tzinfo=UTC)

    try:
        # Only consolidate on Sundays (weekday 6)
        if today.weekday() != 6:
            logger.info(
                "[backup_shared_memory] Not Sunday — skipping weekly consolidation"
            )
            return 0

        # Copy today's backup to weekly path
        year_week = today.strftime("%Y-W%U")
        daily_path = f"{today_str}/dump.json.gz"
        weekly_path = f"weekly/{year_week}/dump.json.gz"

        try:
            # Download daily dump
            daily_data_raw = await asyncio.to_thread(
                lambda: db.storage.from_(BACKUP_BUCKET).download(daily_path)
            )

            # Upload as weekly
            await asyncio.to_thread(
                lambda: db.storage.from_(BACKUP_BUCKET).upload(
                    path=weekly_path,
                    file=daily_data_raw,
                    file_options={
                        "content-type": "application/gzip",
                        "upsert": "true",
                    },
                )
            )
            consolidated += 1
            logger.info(
                "[backup_shared_memory] Consolidated weekly backup: %s", weekly_path
            )

        except Exception as exc:
            logger.warning(
                "[backup_shared_memory] Weekly consolidation copy failed: %s", exc
            )

        # Prune old weeklies
        cutoff_date = today - timedelta(weeks=WEEKLY_RETENTION_WEEKS)
        resp = await asyncio.to_thread(
            lambda: db.storage.from_(BACKUP_BUCKET).list(path="weekly")
        )

        files = resp or []
        for item in files:
            name = item.get("name", "")
            if not name.endswith("/dump.json.gz"):
                continue

            # Extract year-week from path like weekly/2026-W25/dump.json.gz
            parts = name.split("/")
            if len(parts) < 2:
                continue

            week_str = parts[-2]  # e.g., "2026-W25"
            try:
                year_part = int(week_str.split("-W")[0])
                week_part = int(week_str.split("-W")[1])
                # Approximate: first day of that week
                week_start = datetime.strptime(
                    f"{year_part}-W{week_part:02d}-1", "%Y-W%W-%w"
                ).replace(tzinfo=UTC)
            except (ValueError, IndexError):
                continue

            if week_start < cutoff_date:
                await asyncio.to_thread(
                    lambda n=name: db.storage.from_(BACKUP_BUCKET).remove([n])
                )
                logger.info(
                    "[backup_shared_memory] Pruned old weekly backup: %s", name
                )

    except Exception as exc:
        logger.warning(
            "[backup_shared_memory] Weekly consolidation failed (non-fatal): %s", exc
        )

    return consolidated


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------


async def _send_alert(result: BackupResult, message: str) -> None:
    """Send alert via webhook when backup fails or conditions warrant it."""
    if not ALERT_WEBHOOK_URL:
        logger.warning(
            "[backup_shared_memory] ALERT_WEBHOOK_URL not configured — "
            "alert suppressed: %s",
            message,
        )
        return

    try:
        import aiohttp

        payload = {
            "routine": ROUTINE_ID,
            "routine_name": ROUTINE_NAME,
            "exec_id": result.exec_id,
            "status": result.status,
            "row_count": result.row_count,
            "size_bytes": result.size_bytes,
            "sha256": result.sha256,
            "dump_path": result.dump_path,
            "started_at": result.started_at,
            "duration_ms": result.duration_ms,
            "error": result.error,
            "message": message,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                ALERT_WEBHOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    logger.warning(
                        "[backup_shared_memory] Alert webhook returned %d: %s",
                        resp.status,
                        await resp.text(),
                    )
                else:
                    logger.info("[backup_shared_memory] Alert sent successfully")

    except ImportError:
        logger.warning(
            "[backup_shared_memory] aiohttp not available — alert not sent: %s",
            message,
        )
    except Exception as exc:
        logger.error("[backup_shared_memory] Failed to send alert: %s", exc)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backup Shared Memory — logical dump + upload to Supabase Storage.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dump and compute hash without uploading or writing checkpoint.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    parser.add_argument(
        "--restore",
        metavar="DATE",
        help="Restore from a specific dump date (YYYY-MM-DD). Downloads and prints records.",
    )
    parser.add_argument(
        "--list-backups",
        action="store_true",
        help="List available backups in the bucket.",
    )
    return parser


async def _list_backups(db: Any) -> None:
    """List all backups in the storage bucket."""
    resp = await asyncio.to_thread(
        lambda: db.storage.from_(BACKUP_BUCKET).list()
    )

    files = resp or []
    if not files:
        logger.info("No backups found.")
        return

    logger.info(f"{'Path':<40} {'Size':>10} {'Created'}")
    logger.info("-" * 70)
    for item in sorted(files, key=lambda x: x.get("name", ""), reverse=True):
        name = item.get("name", "")
        size = item.get("metadata", {}).get("size", 0)
        created = item.get("created_at", "")
        logger.info(f"{name:<40} {size:>10} {created}")


async def _restore_backup(db: Any, date_str: str) -> None:
    """Download and print a backup from a specific date."""
    path = f"{date_str}/dump.json.gz"

    try:
        raw = await asyncio.to_thread(
            lambda: db.storage.from_(BACKUP_BUCKET).download(path)
        )
        decompressed = gzip.decompress(raw)
        data = json.loads(decompressed)
        logger.info(f"Restored backup from {date_str}: {len(data)} records")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False, default=str))

    except Exception as exc:
        logger.error(f"Failed to restore backup {date_str}: {exc}")
        sys.exit(1)


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    from blu_supabase_client import get_supabase_client

    db = get_supabase_client(use_service_role=True)

    if args.list_backups:
        asyncio.run(_list_backups(db))
        return

    if args.restore:
        asyncio.run(_restore_backup(db, args.restore))
        return

    result = asyncio.run(run_backup(db=db, dry_run=args.dry_run))

    # Print summary
    if args.dry_run:
        logger.info("\n=== DRY RUN ===")
    logger.info(f"Exec ID:      {result.exec_id}")
    logger.info(f"Status:       {result.status}")
    logger.info(f"Row count:    {result.row_count}")
    logger.info(f"Size:         {result.size_bytes} bytes")
    logger.info(f"SHA256:       {result.sha256[:32]}...")
    logger.info(f"Dump path:    {result.dump_path}")
    logger.info(f"Pruned daily: {result.pruned_daily}")
    logger.info(f"Consolidated: {result.consolidated_weekly}")
    if result.error:
        logger.error(f"ERROR:        {result.error}")
    logger.info(f"Duration:     {result.duration_ms}ms")
    logger.info(f"Success:      {result.success}")

    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()

