#!/usr/bin/env python3
"""Generate OpenHouse's read-only profile from the vendored Iceberg REST spec."""

import argparse
import copy
import hashlib
from pathlib import Path

try:
    import yaml
except ModuleNotFoundError:
    raise SystemExit("PyYAML is required: python3 -m pip install PyYAML")


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


def generate(source):
    paths = {}
    used_tags = set()
    for path, methods in KEEP_OPERATIONS.items():
        source_path = source["paths"][path]
        profile_path = {}
        if "parameters" in source_path:
            profile_path["parameters"] = copy.deepcopy(source_path["parameters"])
        for method in methods:
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
        "The Phase 1 read-only profile generated from the vendored Apache Iceberg "
        "REST Catalog OpenAPI specification."
    )
    for key in ("servers", "security"):
        if key in source:
            profile[key] = copy.deepcopy(source[key])
    if "tags" in source:
        profile["tags"] = [
            copy.deepcopy(tag)
            for tag in source["tags"]
            if tag["name"] in used_tags
        ]

    return profile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    source_text = args.source.read_text()
    source = yaml.safe_load(source_text)
    profile = generate(source)
    # Hash the actual vendored bytes, not PyYAML's normalized representation.
    profile["info"]["x-upstream-spec-sha256"] = hashlib.sha256(
        source_text.encode()
    ).hexdigest()
    args.output.write_text(yaml.safe_dump(profile, sort_keys=False, width=100))


if __name__ == "__main__":
    main()
