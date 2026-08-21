#!/usr/bin/env python3
"""Upgrade OpenHouse's checked-in Iceberg REST Phase 1 OpenAPI profile.

Fetches (or reads) an Apache Iceberg REST Catalog OpenAPI document, keeps only
the allowlisted Phase 1 operations plus transitive components, writes
spec/iceberg-rest-catalog-open-api.yaml, and updates the pinned checksum in
services/tables/build.gradle.

The Gradle build never runs this script. It only consumes the checked-in YAML.

Examples:
  python3 spec/upgrade_iceberg_rest_profile.py --tag apache-iceberg-1.11.0
  python3 spec/upgrade_iceberg_rest_profile.py --tag apache-iceberg-1.12.0 --dry-run
  python3 spec/upgrade_iceberg_rest_profile.py --source /tmp/rest-catalog-open-api.yaml \\
      --tag apache-iceberg-1.12.0
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import re
import sys
import urllib.request
from pathlib import Path

try:
    import yaml
except ModuleNotFoundError:
    raise SystemExit("PyYAML is required: python3 -m pip install PyYAML")


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "spec" / "iceberg-rest-catalog-open-api.yaml"
DEFAULT_BUILD_GRADLE = REPO_ROOT / "services" / "tables" / "build.gradle"
UPSTREAM_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/apache/iceberg/{tag}/open-api/rest-catalog-open-api.yaml"
)

# Source of truth for which Iceberg REST resources OpenHouse codegens.
# Adding a Phase 2+ operation means extending this map, re-running the upgrade,
# implementing the handler/controller method, and advertising it in
# OpenHouseIcebergRestApiHandler.SUPPORTED_ENDPOINTS.
KEEP_OPERATIONS = {
    "/v1/config": ("get",),
    "/v1/{prefix}/namespaces/{namespace}/tables": ("get",),
    "/v1/{prefix}/namespaces/{namespace}/tables/{table}": ("get", "head"),
}


def collect_refs(value, refs):
    if isinstance(value, dict):
        reference = value.get("$ref")
        if reference:
            refs.add(reference)
        for child in value.values():
            collect_refs(child, refs)
    elif isinstance(value, list):
        for child in value:
            collect_refs(child, refs)


def resolve_ref(spec, reference):
    value = spec
    for part in reference.removeprefix("#/").split("/"):
        value = value[part.replace("~1", "/").replace("~0", "~")]
    return value


def referenced_components(spec, paths):
    references = set()
    collect_refs(paths, references)

    pending = list(references)
    while pending:
        reference = pending.pop()
        nested = set()
        collect_refs(resolve_ref(spec, reference), nested)
        for candidate in nested - references:
            references.add(candidate)
            pending.append(candidate)

    components = {}
    for reference in sorted(references):
        parts = reference.removeprefix("#/components/").split("/", 1)
        if len(parts) != 2:
            continue
        section, name = parts
        components.setdefault(section, {})[name] = copy.deepcopy(
            spec["components"][section][name]
        )

    # Top-level security declarations name schemes instead of using $ref.
    if spec.get("security") and spec.get("components", {}).get("securitySchemes"):
        components["securitySchemes"] = copy.deepcopy(
            spec["components"]["securitySchemes"]
        )

    return components


def generate_profile(source):
    missing = [path for path in KEEP_OPERATIONS if path not in source.get("paths", {})]
    if missing:
        raise SystemExit(
            "Upstream OpenAPI is missing required Phase 1 paths:\n  - "
            + "\n  - ".join(missing)
        )

    paths = {}
    used_tags = set()
    for path, methods in KEEP_OPERATIONS.items():
        source_path = source["paths"][path]
        profile_path = {}
        if "parameters" in source_path:
            profile_path["parameters"] = copy.deepcopy(source_path["parameters"])
        for method in methods:
            if method not in source_path:
                raise SystemExit(f"Upstream OpenAPI is missing {method.upper()} {path}")
            profile_path[method] = copy.deepcopy(source_path[method])
            used_tags.update(source_path[method].get("tags", []))
        paths[path] = profile_path

    profile = {
        "openapi": source["openapi"],
        "info": copy.deepcopy(source["info"]),
        "paths": paths,
        "components": referenced_components(source, paths),
    }
    profile["info"]["title"] = "OpenHouse Iceberg REST Catalog (read-only profile)"
    profile["info"]["description"] = (
        "OpenHouse Phase 1 read-only subset of the Apache Iceberg REST Catalog OpenAPI."
    )
    for key in ("servers", "security"):
        if key in source:
            profile[key] = copy.deepcopy(source[key])
    if "tags" in source:
        profile["tags"] = [
            copy.deepcopy(tag) for tag in source["tags"] if tag.get("name") in used_tags
        ]
    return profile


def header_comment(tag: str, upstream_url: str, upstream_sha256: str) -> str:
    kept = "\n".join(
        f"#   {method.upper():4} {path}"
        for path, methods in KEEP_OPERATIONS.items()
        for method in methods
    )
    return f"""\
# OpenHouse Phase 1 read-only Iceberg REST Catalog OpenAPI profile.
#
# Source tag: {tag}
# Source URL:
#   {upstream_url}
# Upstream SHA-256: {upstream_sha256}
#
# This file is checked in and held constant. It is not regenerated by the build.
# Regenerate / bump with:
#   python3 spec/upgrade_iceberg_rest_profile.py --tag {tag}
#
# To review what OpenHouse omits from upstream:
#   curl -fsSL \\
#     {upstream_url} \\
#     -o /tmp/iceberg-rest-upstream.yaml
#   diff -u /tmp/iceberg-rest-upstream.yaml spec/iceberg-rest-catalog-open-api.yaml
#
# Kept operations:
{kept}
#

"""


def fetch_upstream(tag: str) -> tuple[str, str, bytes]:
    url = UPSTREAM_URL_TEMPLATE.format(tag=tag)
    with urllib.request.urlopen(url) as response:
        payload = response.read()
    return url, payload.decode("utf-8"), payload


def read_source(path: Path) -> tuple[str, bytes]:
    payload = path.read_bytes()
    return payload.decode("utf-8"), payload


def update_build_gradle_checksum(build_gradle: Path, checksum: str) -> bool:
    text = build_gradle.read_text()
    pattern = re.compile(r'(def icebergRestSpecSha256 = ")[0-9a-f]+(")')
    if not pattern.search(text):
        raise SystemExit(
            f"Could not find icebergRestSpecSha256 assignment in {build_gradle}"
        )
    updated, count = pattern.subn(rf"\g<1>{checksum}\g<2>", text, count=1)
    if count != 1:
        raise SystemExit(f"Failed to update checksum in {build_gradle}")
    if updated == text:
        return False
    build_gradle.write_text(updated)
    return True


def summarize_ops(profile) -> list[str]:
    lines = []
    for path, methods in profile["paths"].items():
        for method in methods:
            if method == "parameters":
                continue
            op_id = methods[method].get("operationId", "?")
            lines.append(f"{method.upper()} {path} ({op_id})")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tag",
        required=True,
        help="Apache Iceberg git tag, e.g. apache-iceberg-1.11.0",
    )
    parser.add_argument(
        "--source",
        type=Path,
        help="Optional local upstream OpenAPI YAML. Default: download from GitHub.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Checked-in profile path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--build-gradle",
        type=Path,
        default=DEFAULT_BUILD_GRADLE,
        help=f"Gradle file with pinned checksum (default: {DEFAULT_BUILD_GRADLE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned write without modifying files",
    )
    args = parser.parse_args()

    if args.source:
        upstream_url = UPSTREAM_URL_TEMPLATE.format(tag=args.tag)
        source_text, source_bytes = read_source(args.source)
    else:
        upstream_url, source_text, source_bytes = fetch_upstream(args.tag)

    upstream_sha256 = hashlib.sha256(source_bytes).hexdigest()
    source = yaml.safe_load(source_text)
    profile = generate_profile(source)
    body = yaml.safe_dump(profile, sort_keys=False, width=100)
    content = header_comment(args.tag, upstream_url, upstream_sha256) + body
    profile_sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()

    print(f"Upstream tag:     {args.tag}")
    print(f"Upstream URL:     {upstream_url}")
    print(f"Upstream SHA-256: {upstream_sha256}")
    print(f"Profile SHA-256:  {profile_sha256}")
    print("Kept operations:")
    for line in summarize_ops(profile):
        print(f"  - {line}")

    if args.dry_run:
        print("Dry run only; no files written.")
        print(
            "After applying, implement any new generated signatures and keep "
            "SUPPORTED_ENDPOINTS in sync with implemented routes."
        )
        return

    args.output.write_text(content)
    checksum_changed = update_build_gradle_checksum(args.build_gradle, profile_sha256)
    print(f"Wrote {args.output.relative_to(REPO_ROOT)}")
    if checksum_changed:
        print(f"Updated checksum in {args.build_gradle.relative_to(REPO_ROOT)}")
    else:
        print(f"Checksum already current in {args.build_gradle.relative_to(REPO_ROOT)}")
    print(
        "Next: ./gradlew :services:tables:icebergRestValidateSpec :services:tables:compileJava\n"
        "Then fix any generated signature drift and confirm SUPPORTED_ENDPOINTS still matches."
    )


if __name__ == "__main__":
    main()
