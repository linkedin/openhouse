#!/usr/bin/env python3
"""Generate reviewable optimizer migrations using the Atlas Community CLI."""

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess


ROOT = Path(__file__).resolve().parents[2]
COMMON = ROOT / "libs/optimizer/optimizer-common"
MIGRATIONS = COMMON / "src/main/resources/db/migration"
GENERATED = ROOT / "build/optimizer/generated-migrations"


def run(*command, cwd=ROOT):
    subprocess.run(command, cwd=cwd, stdin=subprocess.DEVNULL, check=True)


def snapshot():
    files = {}
    for path in MIGRATIONS.iterdir():
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"Not a regular migration file: {path.name}")
        files[path.name] = path.read_bytes()
    return files


def generate(name):
    # Never leave output from an earlier invocation for CI to publish after a failure.
    if GENERATED.exists():
        shutil.rmtree(GENERATED)
    GENERATED.mkdir(parents=True)
    if not shutil.which("atlas"):
        raise RuntimeError("Install Atlas Community; see docs/development/optimizer-schema-migrations.md")
    before = snapshot()
    run("atlas", "migrate", "validate", "--env", "local", cwd=COMMON)
    run("./gradlew", ":services:optimizer:exportEntitySchema")
    run("atlas", "migrate", "diff", "--env", "local", name, cwd=COMMON)
    after = snapshot()
    for filename, content in before.items():
        if filename != "atlas.sum" and after.get(filename) != content:
            raise RuntimeError(f"Generation changed an existing migration: {filename}")
    added = set(after) - set(before)
    versions = []
    for filename in sorted(added):
        if re.fullmatch(rf"U[0-9]{{14}}__{name}\.sql", filename):
            # Flyway Community cannot apply Atlas's generated undo migrations.
            (MIGRATIONS / filename).unlink()
        elif re.fullmatch(rf"V[0-9]{{14}}__{name}\.sql", filename):
            versions.append(filename)
        else:
            raise RuntimeError(f"Unexpected generated file: {filename}")
    if len(versions) > 1:
        raise RuntimeError("Expected at most one new versioned migration")
    run("atlas", "migrate", "hash", "--env", "local", cwd=COMMON)
    run("atlas", "migrate", "validate", "--env", "local", cwd=COMMON)
    if versions:
        latest = max((filename for filename in before if filename.startswith("V")), default="")
        if latest and versions[0].split("__")[0] <= latest.split("__")[0]:
            raise RuntimeError("Generated migration must sort after the existing migrations")
        for filename in [*versions, "atlas.sum"]:
            shutil.copyfile(MIGRATIONS / filename, GENERATED / filename)
        print(f"Generated {versions[0]}; review the SQL before merging.")
    elif (MIGRATIONS / "atlas.sum").read_bytes() != before.get("atlas.sum"):
        raise RuntimeError("Atlas changed checksums without generating a migration")
    else:
        print("Migrations already match the entities; no files generated.")
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"changed={str(bool(versions)).lower()}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", default="sync_entities", help="snake_case migration name")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.name):
        parser.error("--name must be snake_case using lowercase letters and digits")
    generate(args.name)


if __name__ == "__main__":
    main()
