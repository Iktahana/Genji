#!/usr/bin/env python3
"""Commit data/ changes in small batches and optionally push each batch."""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set


DEFAULT_BATCH_SIZE = 1000
DEFAULT_MESSAGE = "chore(data): update entries"
DATA_PATH = "data"


class BatchCommitError(RuntimeError):
    """A user-actionable failure."""


def run_git(
    args: Sequence[str],
    *,
    cwd: Path,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
        check=False,
    )
    if check and result.returncode != 0:
        stderr = (result.stderr or b"").decode("utf-8", "replace").strip()
        detail = f": {stderr}" if stderr else ""
        raise BatchCommitError(f"git {' '.join(args)} failed{detail}")
    return result


def git_output(args: Sequence[str], *, cwd: Path) -> bytes:
    return run_git(args, cwd=cwd).stdout


def nul_paths(output: bytes) -> List[str]:
    return [os.fsdecode(item) for item in output.split(b"\0") if item]


def repository_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise BatchCommitError("run this command inside a Git repository")
    return Path(os.fsdecode(result.stdout.rstrip(b"\n"))).resolve()


def chunks(items: Sequence[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def staged_paths(root: Path) -> List[str]:
    return nul_paths(git_output(["diff", "--cached", "--name-only", "-z"], cwd=root))


def data_changes(root: Path) -> List[str]:
    tracked = nul_paths(
        git_output(
            ["diff", "--no-renames", "--name-only", "-z", "--", DATA_PATH],
            cwd=root,
        )
    )
    untracked = nul_paths(
        git_output(
            ["ls-files", "--others", "--exclude-standard", "-z", "--", DATA_PATH],
            cwd=root,
        )
    )
    return sorted(set(tracked).union(untracked), key=os.fsencode)


def ensure_safe_state(root: Path, *, push: bool) -> None:
    if run_git(["rev-parse", "--verify", "HEAD"], cwd=root, check=False).returncode:
        raise BatchCommitError("the repository needs an initial commit first")

    conflicts = nul_paths(
        git_output(
            ["diff", "--name-only", "--diff-filter=U", "-z", "--", DATA_PATH],
            cwd=root,
        )
    )
    if conflicts:
        raise BatchCommitError("resolve data/ merge conflicts before batching commits")

    staged = staged_paths(root)
    if staged:
        preview = ", ".join(staged[:3])
        suffix = " ..." if len(staged) > 3 else ""
        raise BatchCommitError(
            f"the index already contains {len(staged)} staged path(s): {preview}{suffix}; "
            "commit or unstage them first"
        )

    if push:
        branch = git_output(["symbolic-ref", "--quiet", "--short", "HEAD"], cwd=root)
        if not branch.strip():
            raise BatchCommitError("--push requires a checked-out branch (not detached HEAD)")

        upstream = run_git(
            ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
            cwd=root,
            check=False,
        )
        if upstream.returncode != 0:
            raise BatchCommitError(
                "--push requires an upstream branch; configure one with "
                "git push --set-upstream <remote> <branch>"
            )

        counts = git_output(
            ["rev-list", "--left-right", "--count", "@{upstream}...HEAD"], cwd=root
        ).decode("ascii", "replace").split()
        behind, ahead = (int(value) for value in counts)
        if behind:
            raise BatchCommitError(
                f"the local branch is {behind} commit(s) behind its upstream; sync it first"
            )
        if ahead:
            raise BatchCommitError(
                f"the local branch already has {ahead} unpushed commit(s); push or review "
                "them before running this script with --push"
            )


def commit_batch(
    root: Path,
    batch: Sequence[str],
    *,
    message: str,
    no_verify: bool,
) -> int:
    run_git(["add", "-A", "--", *batch], cwd=root)

    staged = staged_paths(root)
    staged_set: Set[str] = set(staged)
    unexpected = staged_set.difference(batch)
    if unexpected:
        preview = ", ".join(sorted(unexpected, key=os.fsencode)[:3])
        raise BatchCommitError(
            f"unexpected paths were staged ({preview}); stopped before committing"
        )
    if not staged:
        return 0

    command = ["commit", "--only", "-m", message]
    if no_verify:
        command.append("--no-verify")
    command.extend(["--", *batch])
    result = run_git(command, cwd=root, check=False, capture_output=False)
    if result.returncode != 0:
        raise BatchCommitError(
            "git commit failed; this batch remains staged so it can be inspected or retried"
        )

    leftovers = staged_paths(root)
    if leftovers:
        raise BatchCommitError(
            "the commit hook left paths staged; inspect the index before continuing"
        )
    return len(staged)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Commit only data/ changes in file-count batches. With --push, each commit "
            "is pushed immediately instead of uploading all batches at the end."
        )
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"maximum paths per commit (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--message",
        default=DEFAULT_MESSAGE,
        help=f"commit subject prefix (default: {DEFAULT_MESSAGE!r})",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="push after every successful batch commit (requires an upstream branch)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show the plan without staging, committing, or pushing",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="skip commit hooks (use only when explicitly intended)",
    )
    args = parser.parse_args(argv)
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than zero")
    if not args.message.strip() or "\n" in args.message or "\r" in args.message:
        parser.error("--message must be a non-empty single line")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        root = repository_root()
        ensure_safe_state(root, push=args.push)
        changes = data_changes(root)
        if not changes:
            print("No unstaged or untracked changes found under data/.")
            return 0

        total_batches = math.ceil(len(changes) / args.batch_size)
        action = "Would create" if args.dry_run else "Creating"
        push_note = " and push each commit" if args.push else ""
        print(
            f"{action} {total_batches} commit(s) for {len(changes)} data/ path(s)"
            f" (up to {args.batch_size} per commit){push_note}.",
            flush=True,
        )
        if args.dry_run:
            for part, batch in enumerate(chunks(changes, args.batch_size), start=1):
                noun = "file" if len(batch) == 1 else "files"
                subject = f"{args.message} [{part}/{total_batches}] ({len(batch)} {noun})"
                print(f"  {subject}")
            return 0

        committed_batches = 0
        committed_paths = 0
        for part, batch in enumerate(chunks(changes, args.batch_size), start=1):
            noun = "file" if len(batch) == 1 else "files"
            subject = f"{args.message} [{part}/{total_batches}] ({len(batch)} {noun})"
            print(f"\nBatch {part}/{total_batches}: {len(batch)} path(s)", flush=True)
            count = commit_batch(
                root,
                batch,
                message=subject,
                no_verify=args.no_verify,
            )
            if not count:
                print("No remaining change in this batch; skipped.")
                continue
            committed_batches += 1
            committed_paths += count

            if args.push:
                print(f"Pushing batch {part}/{total_batches}...", flush=True)
                pushed = run_git(["push"], cwd=root, check=False, capture_output=False)
                if pushed.returncode != 0:
                    raise BatchCommitError(
                        "push failed after the commit was created; fix the connection or "
                        "remote issue, run git push, then rerun this script"
                    )

        print(
            f"Done: created {committed_batches} commit(s) covering "
            f"{committed_paths} data/ path(s)."
        )
        if not args.push:
            print(
                "Commits were not pushed; push them manually. On future runs, pass "
                "--push from the start to push each batch separately."
            )
        return 0
    except BatchCommitError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
