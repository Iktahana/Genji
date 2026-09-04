---
name: safe-commit-push
description: Review, commit, and optionally push intended Git changes without editing worktree files. Use when the user asks to commit, push, or batch large data-directory changes.
---

# Safe Commit and Push

Operate only on Git state: do not rewrite, format, generate, or repair worktree files as part
of this skill. Treat an explicit request to commit and/or push as authorization for exactly
those Git operations and the requested scope; it does not authorize including unrelated
changes.

## Workflow

1. Resolve the repository root and inspect the branch, upstream, `git status --short`, and
   diffs for the intended paths. Identify pre-existing or unrelated changes and leave them
   untouched.
2. Run read-only validation appropriate to the changed content when it is already available
   and reasonably bounded. Do not run formatters, fix modes, generators, or other commands
   that modify files.
3. Infer concise commit messages from the inspected diff. Use the repository's established
   message convention when one is evident; otherwise use a conventional subject such as
   `fix: ...`, `feat: ...`, `docs: ...`, or `chore(data): ...`.
4. Stage only the intended paths, verify the staged diff, and commit. If the requested scope
   is ambiguous and unrelated changes cannot be separated reliably, stop and ask one focused
   question.
5. Push only when the user requested push. Use the configured upstream and a normal
   fast-forward push. Never force-push, amend, rebase, or set/change an upstream unless the
   user explicitly asks.
6. Report commit hashes, messages, validation performed, push destination/result, and any
   remaining worktree changes.

If a commit hook or push fails, stop with the repository in its resulting recoverable state.
Explain whether paths remain staged or commits remain local. Do not bypass hooks or retry a
state-changing operation unless the user requested that behavior.

## Large `data/` changes in Genji

When `script/batch_commit_data.py` exists and the intended change is under `data/`, prefer it
over manually expanding thousands of paths:

```bash
python3 script/batch_commit_data.py --dry-run
python3 script/batch_commit_data.py --batch-size 1000
python3 script/batch_commit_data.py --batch-size 1000 --push
```

Always run `--dry-run` first and summarize the number of paths and batches. Use `--push` only
when push was requested: it pushes after every successful batch, preventing all batches from
being packed into one final push. The helper refuses an existing staged index, conflicts,
detached HEAD, missing upstream, upstream divergence, and pre-existing unpushed commits. It
commits only `data/`; other worktree changes remain untouched.

The default generated subject is `chore(data): update entries [part/total] (N files)`. Override
the prefix with `--message` only when another subject better describes the data operation.
Avoid `--no-verify` unless the user explicitly authorizes skipping hooks.
