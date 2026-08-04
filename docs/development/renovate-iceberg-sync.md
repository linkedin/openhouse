# Renovate: linkedin/iceberg version sync

OpenHouse depends on the LinkedIn Iceberg fork (`com.linkedin.iceberg`, published
to Maven Central) and deliberately pins **two minor lines at once**:

| Source of truth | Line | Drives |
| --- | --- | --- |
| `iceberg_1_2_version` in `build.gradle` | `1.2.x` | Spark 3.1 stack, `iceberg-1.2` integrations, `datalayout` |
| `iceberg_1_5_version` in `build.gradle` | `1.5.x` | Spark 3.5 stack, `iceberg-1.5` integrations, services |

Renovate ([`.github/renovate.json`](../../.github/renovate.json)) keeps each pin at
the newest release **within its own line** and opens a PR per line when a newer
version exists. It runs hourly via the self-hosted
[`renovate.yml`](../../.github/workflows/renovate.yml) workflow.

## Why Renovate (and not native Dependabot)

Native Dependabot cannot express this repo's setup:

1. **Two minor lines of the same artifacts.** `iceberg-core`, `iceberg-common`,
   `iceberg-data`, and `iceberg-bundled-guava` are each pinned at both `1.2.x`
   and `1.5.x`. Dependabot's `ignore` rules are keyed per-artifact, so it would
   try to converge both pins onto a single latest version, breaking one line.
   Renovate's `matchCurrentVersion` evaluates each occurrence against its own
   current value, so the `1.2.0.x` pin and the `1.5.2.x` pin get independent
   ceilings.
2. **Cross-file `ext` indirection.** Versions are defined once in the root
   `build.gradle` and read elsewhere via `rootProject.ext.…` and `buildSrc`
   convention plugins. Instead of relying on a parser to follow that chain, a
   Renovate `customManager` (regex) updates the source-of-truth lines directly.

`enabledManagers: ["custom.regex"]` scopes Renovate to **only** these three
version strings — it does not touch any other dependency. GitHub Actions updates
remain handled by the existing [`dependabot.yml`](../../.github/dependabot.yml).

## How the line caps work

Each line is held by a `packageRule` keyed on the current version, e.g. for the
1.5 line:

```json
{
  "matchPackageNames": ["com.linkedin.iceberg:iceberg-core"],
  "matchCurrentVersion": "/^1\\.5\\./",
  "allowedVersions": "/^1\\.5\\./"
}
```

The 4-part LinkedIn versions (`1.5.2.11`) are compared with Renovate's `maven`
versioning, and release candidates (`1.5.2.0-rc1`) are skipped as unstable.

## One-time setup

Create a repository (or org) secret named **`RENOVATE_TOKEN`** so Renovate can
open PRs **and** so CI runs on them — PRs opened with the default `GITHUB_TOKEN`
do not trigger other workflows. Use one of:

- a classic PAT with `repo` + `workflow` scope, or
- a fine-grained PAT / GitHub App installation token with read+write on
  **Contents** and **Pull requests**.

Without `RENOVATE_TOKEN` the workflow falls back to `GITHUB_TOKEN`: PRs are still
created, but their CI must be re-run manually.

## Verifying / running on demand

- Trigger **Actions → "Renovate (linkedin/iceberg sync)" → Run workflow** with
  the `Dry run` option to see what Renovate would do without opening PRs.
- Validate config changes locally:
  `npx --yes --package renovate -- renovate-config-validator .github/renovate.json`
