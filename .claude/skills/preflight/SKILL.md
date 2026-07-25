---
name: preflight
description: "Run the exact CI lint + test commands locally before pushing a branch or opening a PR. Triggers on `git push`, `gh pr create`, or any moment code is about to be handed to CI. Prior PRs failed on trivial ruff findings (`F401` unused imports, `PERF403` for-loop-into-dict) — a 30-second local run catches every one of these before they burn a CI cycle."
---

Run these three commands in parallel and fix everything before pushing. They are the exact commands CI runs (see `.github/workflows/`).

## The three checks

```bash
poetry run ruff check .                                # Python lint
poetry run pytest tests/ -q --tb=short                 # Python tests
cd ui && npm run lint && npm run build                 # UI lint + build (tsc + vite)
```

Run them in parallel — three separate Bash tool calls in one message — because they're independent. Any failure blocks the push.

## When to invoke

Before **every** one of:

- `git push` (especially the first push of a branch)
- `gh pr create`
- Amending a commit that has already been pushed and is about to be re-pushed
- Any moment you're about to hand code to CI

Not before every commit — commits are cheap and reversible. Push is the moment CI sees the code, so push is the trigger.

## What to fix, what to argue

- **Ruff `[*]` findings** (auto-fixable) → `poetry run ruff check . --fix` and re-run. Do not commit the fix as a separate PR.
- **Ruff findings without `[*]`** → fix by hand. Don't add `# noqa` unless the rule genuinely doesn't apply to this case; write one sentence explaining why in the pragma comment.
- **Test failures** → these are real. Do not push. Fix or ask.
- **UI type errors** → `npm run build` runs `tsc -b`; a type error there is a real bug. Fix.

## Common findings this project trips on

- `F401` — unused import. Test files that used `pytest` for fixtures often stop needing it after a refactor.
- `PERF403` — a `for k, v in x.items(): out[k] = v` loop should be `dict(x)` or a dict comprehension.
- Newly-added `Union[X, None]` should be `X | None` (project uses 3.12+ syntax throughout).

## Exit criteria

`All checks passed!` from ruff, green pytest tail, `built in Xms` from vite. Only then push.
