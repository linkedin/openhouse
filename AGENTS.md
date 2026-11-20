# AGENTS.md

Instructions for AI coding agents working on OpenHouse. Humans: see [CONTRIBUTING.md](CONTRIBUTING.md); local setup
is in [SETUP.md](SETUP.md).

## Pull requests

Maintainer review time is this project's bottleneck: large PRs wait days for review and are the most likely to be
closed without merging. Shape every PR so a maintainer can review it in one ~10-minute sitting.

### PR shape

- **One purpose per PR.** If you cannot state the change in one sentence, split it.
- **Budget: 400 counted lines**, i.e. added + deleted lines of hand-written, non-test code (see "Pull Request Size"
  in CONTRIBUTING.md for what is excluded). Aim for 200 or less when touching concurrency, authorization, or table
  deletion/retention.
- **At most 2 walls of code**, where a wall is a run of 50+ consecutive new lines. Everything else should be small
  edits a reviewer can verify at a glance.
- **At most one new design decision per PR**; everything else follows existing patterns.
- **Mechanical changes ship alone, first.** Refactors, renames, formatting, and dependency bumps go in their own PR
  ahead of the behavior change, and the description says how they were produced (e.g., "mechanical: IntelliJ
  rename") so reviewers can skim.
- **Low-risk code gets a lighter review.** Paths marked `review-risk=low` in `.gitattributes` hold code no deployment
  runs; the `PR size` check doesn't count them. Keep low-risk and production changes in separate PRs and start the
  description with "Low-risk: <why no deployment can be affected>". Don't add paths to that list yourself; ask a
  maintainer.
- **Plan multi-part work before writing code.** With push access to linkedin/openhouse, create a stack first with
  `gh stack` and follow [`.agents/skills/stacked-prs/SKILL.md`](.agents/skills/stacked-prs/SKILL.md). From a fork,
  open the PRs one at a time in dependency order and list the plan in the first PR's description.
- **Review your own diff first.** Read all of it, comment inline where intent isn't obvious, and remove debug code
  and unrelated churn before requesting review.

The `PR size` check (`.github/workflows/pr-size.yml`) enforces the budget and the wall limit; run it locally with
`python3 .github/scripts/pr_size_check.py --base upstream/main`. If a PR truly cannot be split, add a "Why this can't
be split" section to the description and ask a maintainer to add the `size-exception` label.

### PR description

Use [`.github/pull_request_template.md`](.github/pull_request_template.md). Keep it under ~25 lines and don't
restate the diff:

- **Summary**: the linked issue, the problem, the approach, and alternatives you rejected (1-3 sentences).
- **Changes**: tick the boxes that apply. In the details, give the review order (what to read first, which parts are
  mechanical) and "Look hard at:" 1-3 locations, each with a specific question for the reviewer.
- **Testing Done**: commands run with trimmed output, tests that fail without the change, what was *not* verified,
  and which parts were AI-generated (you reviewed every line).
