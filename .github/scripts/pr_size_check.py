#!/usr/bin/env python3
"""PR size check: fails pull requests that are too large to review well.

Counted lines are added + deleted lines in hand-written, non-test files. Not counted: tests,
docs, lockfiles, binary files, files deleted outright, vendor/ and third_party/ directories,
paths marked linguist-generated or linguist-vendored in .gitattributes, and low-risk paths marked
review-risk=low in .gitattributes (code production never runs, reviewed for blast radius only).
A "wall" is a run of WALL_LINES or more consecutive new lines in a counted file.

The check fails when counted lines exceed BUDGET_LINES or walls exceed MAX_WALLS, unless the PR
has the EXCEPTION_LABEL label and its description explains "Why this can't be split".

CI (on the PR merge commit):  python3 .github/scripts/pr_size_check.py --base HEAD^1 --head HEAD
Local:                        python3 .github/scripts/pr_size_check.py --base origin/<default branch>
"""
import argparse
import json
import os
import re
import subprocess
import sys

# Repository settings. Everything below this block is repository-independent.
BUDGET_LINES = 400
WALL_LINES = 50
MAX_WALLS = 2
EXCEPTION_LABEL = "size-exception"
HOW_TO_SPLIT = 'Split it into smaller PRs (see "Pull Request Size" in CONTRIBUTING.md).'
HOW_TO_EXCEPT = (
    'If it truly cannot be split, add a "Why this can\'t be split" section to the PR description and ask a '
    "maintainer to add the `%s` label." % EXCEPTION_LABEL
)

JUSTIFICATION = re.compile(r"why\s+this\s+(?:can['’]?t|cannot|can\s+not)\s+be\s+split", re.IGNORECASE)
TEST_DIR = re.compile(
    r"^(tests?|testfixtures|it|itest|integrationtest)$|test-fixtures|-itest|_itest|acceptance[-_]test", re.IGNORECASE
)
TEST_FILE = re.compile(
    r"(Test|Tests|[a-z0-9]IT|ITCase)\.(java|scala|kt|groovy)$|(Spec|Suite)\.scala$"
    r"|^test_.*\.py$|_test\.(py|go)$|\.(test|spec)\.[jt]sx?$"
)
NOT_CODE = re.compile(
    r"\.(md|markdown|rst|adoc|txt|csv|tsv)$"
    r"|(^|/)(package-lock\.json|pnpm-lock\.yaml|[^/]*\.lock|[^/]*\.lockfile)$",
    re.IGNORECASE,
)
VENDORED = re.compile(r"(^|/)(vendor|third[-_]party|node_modules)/")
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def git(*args):
    out = subprocess.run(
        ["git", "-c", "core.quotePath=false"] + list(args), stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
    )
    if out.returncode != 0:
        sys.exit("git %s failed: %s" % (" ".join(args), out.stderr.decode("utf-8", "replace").strip()))
    return out.stdout.decode("utf-8", "replace")


def is_test(path):
    parts = path.split("/")
    return any(TEST_DIR.search(d) for d in parts[:-1]) or bool(TEST_FILE.search(parts[-1]))


def changed_files(base, head):
    """[{path, old, added, deleted, binary, status}] for base..head, with rename detection."""
    statuses = {}
    tokens = git("diff", "--no-color", "--name-status", "-z", "-M", base, head).split("\0")
    i = 0
    while i < len(tokens) - 1:
        status = tokens[i]
        if status[:1] in ("R", "C"):
            statuses[tokens[i + 2]] = status[0]
            i += 3
        else:
            statuses[tokens[i + 1]] = status[0]
            i += 2

    files = []
    tokens = git("diff", "--no-color", "--numstat", "-z", "-M", base, head).split("\0")
    i = 0
    while i < len(tokens) - 1:
        added, deleted, path = tokens[i].split("\t", 2)
        old = path
        if not path:  # rename/copy: "added\tdeleted\t" NUL old NUL new
            old, path = tokens[i + 1], tokens[i + 2]
            i += 3
        else:
            i += 1
        binary = added == "-"
        files.append(
            {
                "path": path,
                "old": old,
                "added": 0 if binary else int(added),
                "deleted": 0 if binary else int(deleted),
                "binary": binary,
                "status": statuses.get(path, "M"),
            }
        )
    return files


def path_attributes(paths):
    """{path: {"generated", "low-risk"}} from .gitattributes: linguist-generated/-vendored and review-risk=low."""
    if not paths:
        return {}
    out = subprocess.run(
        ["git", "check-attr", "-z", "--stdin", "linguist-generated", "linguist-vendored", "review-risk"],
        input="".join(p + "\0" for p in paths).encode("utf-8"),
        stdout=subprocess.PIPE,
        check=True,
    ).stdout.decode("utf-8", "replace")
    tokens = out.split("\0")
    attrs = {}
    for i in range(0, len(tokens) - 2, 3):
        path, name, value = tokens[i], tokens[i + 1], tokens[i + 2]
        if name == "review-risk" and value == "low":
            attrs.setdefault(path, set()).add("low-risk")
        elif name != "review-risk" and value in ("set", "true"):
            attrs.setdefault(path, set()).add("generated")
    return attrs


def classify(files):
    attrs = path_attributes([f["path"] for f in files if f["status"] != "D"])
    for f in files:
        flags = attrs.get(f["path"], set())
        if f["binary"]:
            f["kind"] = "binary"
        elif f["status"] == "D":
            f["kind"] = "deleted"
        elif "generated" in flags or VENDORED.search(f["path"]):
            f["kind"] = "generated/vendored"
        elif "low-risk" in flags:
            f["kind"] = "low-risk"
        elif NOT_CODE.search(f["path"]):
            f["kind"] = "docs/lockfile"
        elif is_test(f["path"]):
            f["kind"] = "test"
        else:
            f["kind"] = "counted"
    return files


def find_walls(base, head, files):
    """Runs of >= WALL_LINES consecutive added lines in counted files: [(path, first_line, length)]."""
    counted = [f for f in files if f["kind"] == "counted" and f["added"] >= WALL_LINES]
    if not counted:
        return []
    pathspecs = sorted({":(literal)" + p for f in counted for p in (f["path"], f["old"])})
    args = ["diff", "-U0", "-M", "--no-color", "--no-ext-diff", "--src-prefix=a/", "--dst-prefix=b/", base, head, "--"]
    patch = git(*(args + pathspecs))
    walls, path, run, start, line = [], None, 0, 0, 0
    wanted = {f["path"] for f in counted}

    def flush():
        if path in wanted and run >= WALL_LINES:
            walls.append((path, start, run))

    in_hunk = False
    for text in patch.split("\n"):
        if text.startswith("diff --git "):
            flush()
            path, run, in_hunk = None, 0, False
        elif not in_hunk and text.startswith("+++ "):
            path = text[6:] if text.startswith("+++ b/") else None
        elif text.startswith("@@"):
            flush()
            run, in_hunk = 0, True
            line = int(HUNK.match(text).group(1))
        elif in_hunk and text.startswith("+"):
            if run == 0:
                start = line
            run += 1
            line += 1
        elif in_hunk and text.startswith("-"):
            flush()
            run = 0
    flush()
    return walls


def pr_context():
    """(labels, body) of the PR from the GitHub Actions event payload; empty outside CI."""
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path or not os.path.exists(event_path):
        return set(), ""
    with open(event_path) as f:
        pr = json.load(f).get("pull_request") or {}
    return {label["name"] for label in pr.get("labels", [])}, pr.get("body") or ""


def has_justification(body):
    """True when the body has a "Why this can't be split" heading or line followed by real text."""
    body = re.sub(r"<!--.*?-->", "", body, flags=re.DOTALL)
    match = JUSTIFICATION.search(body)
    if not match:
        return False
    rest = body[match.end() :].lstrip(" \t:*_?")
    section = re.split(r"^\s*#", rest, maxsplit=1, flags=re.MULTILINE)[0]
    return len(re.sub(r"\W", "", section)) >= 10


def size_label(lines):
    for name, limit in (("XS", 10), ("S", 30), ("M", 100), ("L", 500), ("XL", 1000)):
        if lines < limit:
            return name
    return "XXL"


def measure(base, head):
    merge_base = git("merge-base", base, head).strip()
    files = classify(changed_files(merge_base, head))
    lines_by_kind = {}
    for f in files:
        lines_by_kind[f["kind"]] = lines_by_kind.get(f["kind"], 0) + f["added"] + f["deleted"]
    return {
        "files": files,
        "counted_lines": lines_by_kind.get("counted", 0),
        "test_lines": lines_by_kind.get("test", 0),
        "low_risk_lines": lines_by_kind.get("low-risk", 0),
        "walls": find_walls(merge_base, head, files),
    }


def report(result, labels, body):
    lines, walls = result["counted_lines"], result["walls"]
    over_budget, too_many_walls = lines > BUDGET_LINES, len(walls) > MAX_WALLS
    excepted = EXCEPTION_LABEL in labels
    justified = has_justification(body)
    if not (over_budget or too_many_walls):
        verdict = "pass"
    elif excepted and justified:
        verdict = "exception"
    else:
        verdict = "fail"

    out = [
        "## PR size: %s (%d counted lines, %d wall%s)"
        % (size_label(lines), lines, len(walls), "" if len(walls) == 1 else "s"),
        "",
        "| | This PR | Budget |",
        "|---|---:|---:|",
        "| Counted lines (hand-written, non-test) | %d | %d |" % (lines, BUDGET_LINES),
        "| Walls (runs of %d+ new lines) | %d | %d |" % (WALL_LINES, len(walls), MAX_WALLS),
        "| Test lines (not counted) | %d | – |" % result["test_lines"],
        "| Low-risk lines (`review-risk=low`, not counted) | %d | – |" % result["low_risk_lines"],
        "",
    ]
    if result["low_risk_lines"] and not lines:
        out += [
            "Only low-risk paths changed: review for blast radius (secrets, production endpoints, cost, shared CI) "
            "rather than line by line.",
            "",
        ]
    counted = sorted(
        (f for f in result["files"] if f["kind"] == "counted"), key=lambda f: -(f["added"] + f["deleted"])
    )
    if counted:
        out += ["Counted files:", ""]
        out += ["- `%s` +%d −%d" % (f["path"], f["added"], f["deleted"]) for f in counted[:25]]
        if len(counted) > 25:
            out.append("- … %d more" % (len(counted) - 25))
        out.append("")
    if walls:
        out += ["Walls:", ""] + ["- `%s:%d` (%d new lines)" % w for w in walls] + [""]
    skipped = {}
    for f in result["files"]:
        if f["kind"] not in ("counted", "test"):
            skipped[f["kind"]] = skipped.get(f["kind"], 0) + 1
    if skipped:
        out += ["Not counted: " + ", ".join("%d %s" % (n, k) for k, n in sorted(skipped.items())), ""]

    if verdict == "exception":
        out.append(
            "Over budget; allowed by the `%s` label and the "
            '"Why this can\'t be split" section.' % EXCEPTION_LABEL
        )
    elif verdict == "fail":
        problems = []
        if over_budget:
            problems.append("%d counted lines (budget %d)" % (lines, BUDGET_LINES))
        if too_many_walls:
            problems.append("%d walls (max %d)" % (len(walls), MAX_WALLS))
        out.append("**Too large to review well: %s.** %s %s" % (" and ".join(problems), HOW_TO_SPLIT, HOW_TO_EXCEPT))
        if excepted and not justified:
            out.append("")
            out.append(
                "The `%s` label is set, but the description has no "
                '"Why this can\'t be split" section.' % EXCEPTION_LABEL
            )
    return verdict, "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", required=True, help="base ref; the diff starts at merge-base(base, head)")
    parser.add_argument("--head", default="HEAD", help="head ref (default: HEAD)")
    args = parser.parse_args()

    labels, body = pr_context()
    verdict, text = report(measure(args.base, args.head), labels, body)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text)
    if verdict == "fail":
        print("::error title=PR size::Over the PR size budget; see the job summary for details and how to proceed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
