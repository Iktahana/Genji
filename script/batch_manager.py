#!/usr/bin/env python3
"""
Batch manager for adding examples to dictionary entries.

Manages checkpoint state and generates batches for parallel processing.
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone
import argparse


def get_data_root():
    """Get the data directory."""
    return Path(__file__).parent.parent / "data"


def get_checkpoint_dir():
    """Get or create checkpoint directory."""
    cp_dir = Path(__file__).parent / ".checkpoints"
    cp_dir.mkdir(exist_ok=True)
    return cp_dir


def get_checkpoint_file():
    """Get the checkpoint file path."""
    return get_checkpoint_dir() / "progress.json"


def load_checkpoint():
    """Load checkpoint data or create empty."""
    cp_file = get_checkpoint_file()
    if cp_file.exists():
        with open(cp_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        "completed_batches": [],
        "failed_batches": [],
        "last_updated": datetime.now(timezone.utc).isoformat()
    }


def save_checkpoint(checkpoint):
    """Save checkpoint data."""
    checkpoint["last_updated"] = datetime.now(timezone.utc).isoformat()
    cp_file = get_checkpoint_file()
    tmp_file = cp_file.with_suffix('.json.tmp')
    with open(tmp_file, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    tmp_file.rename(cp_file)


def get_all_files(rows, batch_size=75):
    """
    Scan all JSON files in specified rows and return batches.

    Args:
        rows: list of row names (あ, い, う, え, お)
        batch_size: number of files per batch

    Returns:
        list of (batch_id, [file_paths])
    """
    data_root = get_data_root()
    batches = []
    batch_id = 0
    current_batch = []

    for row in rows:
        row_dir = data_root / row
        if not row_dir.exists():
            print(f"Warning: {row_dir} does not exist", file=sys.stderr)
            continue

        # Get all JSON files for this row
        files = sorted(row_dir.glob("*.json"))

        for file_path in files:
            current_batch.append(str(file_path))

            if len(current_batch) >= batch_size:
                batches.append((f"batch_{batch_id:03d}", current_batch.copy()))
                batch_id += 1
                current_batch = []

    # Add remaining files
    if current_batch:
        batches.append((f"batch_{batch_id:03d}", current_batch))

    return batches


def filter_pending_batches(batches, checkpoint):
    """Filter out completed batches."""
    completed = set(checkpoint.get("completed_batches", []))
    return [(bid, files) for bid, files in batches if bid not in completed]


def list_pending_batches(rows, batch_size=75):
    """List pending batches and output as JSON."""
    checkpoint = load_checkpoint()
    batches = get_all_files(rows, batch_size)
    pending = filter_pending_batches(batches, checkpoint)

    output = {
        "total_batches": len(batches),
        "completed_batches": len(checkpoint.get("completed_batches", [])),
        "pending_batches": len(pending),
        "batches": [
            {
                "batch_id": bid,
                "file_count": len(files),
                "files": files
            }
            for bid, files in pending  # Return all pending batches
        ]
    }

    return output


def mark_batch_done(batch_id, checkpoint):
    """Mark a batch as completed."""
    if batch_id not in checkpoint.get("completed_batches", []):
        if "completed_batches" not in checkpoint:
            checkpoint["completed_batches"] = []
        checkpoint["completed_batches"].append(batch_id)
    save_checkpoint(checkpoint)


def mark_batch_failed(batch_id, error_msg, checkpoint):
    """Mark a batch as failed."""
    if "failed_batches" not in checkpoint:
        checkpoint["failed_batches"] = []

    # Check if already in failed list
    if not any(b["id"] == batch_id for b in checkpoint["failed_batches"]):
        checkpoint["failed_batches"].append({
            "id": batch_id,
            "error": error_msg,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    save_checkpoint(checkpoint)


def get_stats(rows, batch_size=75):
    """Get progress statistics."""
    checkpoint = load_checkpoint()
    batches = get_all_files(rows, batch_size)
    pending = filter_pending_batches(batches, checkpoint)

    total_files = sum(len(files) for _, files in batches)
    completed_files = 0

    # Count completed files (approximation)
    completed_batch_count = len(checkpoint.get("completed_batches", []))
    completed_files = completed_batch_count * batch_size

    stats = {
        "total_batches": len(batches),
        "completed_batches": len(checkpoint.get("completed_batches", [])),
        "pending_batches": len(pending),
        "total_files": total_files,
        "completed_files": min(completed_files, total_files),
        "failed_batches": len(checkpoint.get("failed_batches", [])),
        "last_updated": checkpoint.get("last_updated")
    }

    return stats


def main():
    parser = argparse.ArgumentParser(description="Batch manager for example addition")
    parser.add_argument("--action", choices=["list-pending", "mark-done", "stats", "mark-failed"],
                      required=True, help="Action to perform")
    parser.add_argument("--rows", default="あ,い,う,え,お",
                      help="Rows to process (comma-separated)")
    parser.add_argument("--batch-size", type=int, default=75,
                      help="Files per batch")
    parser.add_argument("--batch-id", help="Batch ID (for mark-done, mark-failed)")
    parser.add_argument("--error", help="Error message (for mark-failed)")

    args = parser.parse_args()

    rows = [r.strip() for r in args.rows.split(",")]

    if args.action == "list-pending":
        result = list_pending_batches(rows, args.batch_size)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.action == "mark-done":
        if not args.batch_id:
            print("Error: --batch-id required for mark-done", file=sys.stderr)
            sys.exit(1)
        cp = load_checkpoint()
        mark_batch_done(args.batch_id, cp)
        print(f"Marked {args.batch_id} as completed")

    elif args.action == "mark-failed":
        if not args.batch_id:
            print("Error: --batch-id required for mark-failed", file=sys.stderr)
            sys.exit(1)
        error_msg = args.error or "Unknown error"
        cp = load_checkpoint()
        mark_batch_failed(args.batch_id, error_msg, cp)
        print(f"Marked {args.batch_id} as failed: {error_msg}")

    elif args.action == "stats":
        stats = get_stats(rows, args.batch_size)
        print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
