"""Reject edits to merged optimizer migrations and additions with older versions."""

import argparse
from pathlib import Path
import re
import subprocess
import sys


MIGRATIONS = Path("libs/optimizer/optimizer-common/src/main/resources/db/migration")
VERSIONED_SQL = re.compile(r"V([0-9]+(?:[._][0-9]+)*)__.+\.sql\Z")


def git(repository, *args):
    return subprocess.check_output(
        ["git", "-C", str(repository), *args], stderr=subprocess.PIPE
    )


def version(path):
    match = VERSIONED_SQL.fullmatch(path.name)
    if match is None:
        raise ValueError(f"Invalid versioned migration name: {path.name}")
    parts = [int(part) for part in re.split(r"[._]", match.group(1))]
    # Flyway treats 1, 01, 1.0 and 1_0 as the same version.
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()
    return tuple(parts)


def is_migration(path):
    return path.name.startswith("V") and path.suffix == ".sql"


def check(repository, base):
    try:
        commit = git(repository, "rev-parse", "--verify", "--end-of-options", f"{base}^{{commit}}")
    except subprocess.CalledProcessError as error:
        raise ValueError(
            f"Base commit is unavailable: {base}. Fetch the intended base before running this check."
        ) from error
    commit = commit.decode().strip()
    entries = git(repository, "ls-tree", "-r", "-z", commit, "--", MIGRATIONS.as_posix())
    previous = {}
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        path = Path(name.decode())
        if is_migration(path):
            mode, kind, object_id = metadata.decode().split()
            if kind != "blob" or mode not in ("100644", "100755"):
                raise ValueError(f"Base migration is not a regular file: {path}")
            previous[path] = object_id

    directory = repository / MIGRATIONS
    current = {
        path.relative_to(repository): path
        for path in directory.rglob("*")
        if is_migration(path)
    }
    errors = []
    for path, object_id in previous.items():
        file = current.get(path)
        if file is None:
            errors.append(f"Merged migration was deleted or renamed: {path}")
        elif file.is_symlink() or not file.is_file():
            errors.append(f"Migration must remain a regular file: {path}")
        elif git(repository, "hash-object", "--no-filters", "--", str(file)).decode().strip() != object_id:
            errors.append(f"Merged migration was modified: {path}")

    newest = max((version(path) for path in previous), default=None)
    versions = {}
    for path, file in sorted(current.items()):
        if file.is_symlink() or not file.is_file():
            errors.append(f"Migration must be a regular file: {path}")
            continue
        number = version(path)
        if number in versions:
            errors.append(f"Duplicate migration version: {versions[number]} and {path}")
        versions[number] = path
        if path not in previous and newest is not None and number <= newest:
            errors.append(
                f"New migration must sort after the newest migration in {base}: {path}. "
                "Rebase and give only the new migration a later version."
            )
    if errors:
        raise ValueError("\n".join(errors))
    return len(previous), len(current) - len(previous)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="Fetched PR base commit or local Git ref")
    args = parser.parse_args()
    try:
        repository = Path(git(Path.cwd(), "rev-parse", "--show-toplevel").decode().strip())
        unchanged, added = check(repository, args.base)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Optimizer migration history check failed:\n{error}", file=sys.stderr)
        return 1
    print(f"Optimizer migration history OK: {unchanged} immutable migrations, {added} additions.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
