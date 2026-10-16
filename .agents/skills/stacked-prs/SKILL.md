---
name: stacked-prs
description: >
  Split multi-part work into a stack of small, dependent pull requests with `gh stack` (GitHub stacked PRs).
  Use when a change would exceed the PR size budget in AGENTS.md, when the "PR size" check fails, when asked to
  split, stack, or layer work for review, or when a stack is checked out.
---

# Stacked PRs in linkedin/openhouse

A stack is an ordered chain of branches rooted on `main`. Each branch has one PR whose base is the branch below it,
so reviewers see only that layer's diff, and the approved stack merges in one step. Stacks need every branch in
linkedin/openhouse itself (push access); cross-fork stacks are not supported. From a fork, open the PRs one at a
time instead. Adapted from GitHub's [gh-stack agent skill](https://github.com/github/gh-stack/tree/main/skills/gh-stack)
(MIT).

## Setup (once per clone)

```bash
gh extension install github/gh-stack    # or: gh extension upgrade stack
git config rerere.enabled true           # remember conflict resolutions across rebases
git config remote.pushDefault upstream   # the remote that points at linkedin/openhouse
```

Without `remote.pushDefault`, pass `--remote <name>` to `push`, `submit`, `sync`, and `rebase`; clones of this
project usually have both a fork and an upstream remote.

## Plan the layers before writing code

- Decide the layers first, then write code into them. Writing everything on one branch and splitting it afterwards
  is the failure mode to avoid.
- One concern per layer. A layer you cannot describe in one sentence is two layers.
- Order by dependency: foundations at the bottom, code that uses them above. Every layer must build and pass tests
  on its own.
- Prefer thin end-to-end slices (behind a flag if needed) over model → service → client layers; a reviewer cannot
  judge a layer without seeing how it is used.
- Mechanical changes (refactors, renames, formatting, dependency bumps) are their own bottom layer.
- Each layer fits the PR size budget in AGENTS.md ("PR shape").
- Unrelated work gets its own stack.
- Branch names share a topic: `<user>/<topic>-<concern>`, for example `jdoe/retention-api` and `jdoe/retention-job`.

## Non-interactive use

`gh stack` opens prompts and full-screen UIs when attached to a terminal. Always use these forms:

| Use | Never run bare | Why |
|---|---|---|
| `gh stack init <branch>...` | `gh stack init` | prompts for branch names |
| `gh stack add <branch>` | `gh stack add` | prompts for a name |
| `gh stack view --json` | `gh stack view` | opens a UI |
| `gh stack submit --auto` | `gh stack submit` | prompts for each PR title |
| `gh stack merge <pr> --yes` | `gh pr merge` | `gh pr merge` cannot merge a stack |
| `gh stack up` / `down` / `top` / `bottom` | `gh stack switch`, `gh stack modify` | menu-only |

## Core loop

```bash
gh stack init jdoe/retention-api            # first layer, based on main
git add <this layer's files> && git commit -m "Add retention policy to the tables API"
gh stack add jdoe/retention-job             # next layer, branched from the current one
git add <this layer's files> && git commit -m "Enforce retention policy in the retention job"
gh stack submit --auto                      # push every branch and open draft PRs
gh stack view --json                        # confirm
```

Stage each layer's files explicitly with `git add <paths>`. `gh stack submit --auto` opens PRs with generated titles
and bodies; write each one with `gh pr edit <n> --title <title> --body-file <file>` (AGENTS.md "PR description").

## Changing a lower layer

```bash
gh stack down                  # or: gh stack checkout <branch>
git add <files> && git commit -m "Handle tables without a retention policy"
gh stack rebase --upstack      # replay every layer above onto the change
gh stack top
gh stack push
```

Never commit a lower layer's change on the top branch.

## Merging in this repository

- Each layer runs the same required checks as a PR to `main` (`build-run-tests / Build and Run Tests`) and needs
  its own approval, with review threads resolved.
- Pushes dismiss existing approvals here, and merging one layer rebases the layers above it. Once every layer is
  approved, merge the whole stack at once: `gh stack merge <top-pr> --yes` lands that PR and every unmerged PR below
  it.
- Auto-merge is not available for stacked PRs yet.
- After anything merges, run `gh stack sync` to rebase and push what is left.

## When a command fails

- Exit 2, not in a stack: `gh stack init <branch>` or `gh stack checkout <pr>`.
- Exit 3, rebase conflict: resolve, `git add`, then `gh stack rebase --continue` (or `--abort`).
- Exit 9, stacked PRs unavailable: tell the user; do not work around it.
- Push rejected with "Permission denied": git or `gh` is using an account without push access to linkedin/openhouse
  (for example an enterprise-managed account). Use an account with push access, or open the PRs from a fork.
- Anything else: `gh stack <command> --help` is authoritative.
