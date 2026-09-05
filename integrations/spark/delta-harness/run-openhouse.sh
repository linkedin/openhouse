#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
JAVA_HOME="${JAVA17_HOME:-${JAVA_HOME:-}}"
if [[ -z "$JAVA_HOME" ]]; then
  echo "Set JAVA17_HOME or JAVA_HOME to a JDK 17 installation." >&2
  exit 2
fi
export JAVA_HOME

JAVA_VERSION="$("$JAVA_HOME/bin/java" -version 2>&1 | sed -n '1s/.*version "\([0-9][0-9]*\).*/\1/p')"
if [[ "$JAVA_VERSION" != "17" ]]; then
  echo "The delta harness requires JDK 17; $JAVA_HOME reports Java $JAVA_VERSION." >&2
  exit 2
fi

cd "$REPO_ROOT"
if (( $# == 0 )); then
  exec ./gradlew --no-daemon \
    :integrations:spark:openhouse-spark-delta-harness_2.12:runOpenHouse
fi

printf -v FILTERS ' %q' "$@"
exec ./gradlew --no-daemon \
  :integrations:spark:openhouse-spark-delta-harness_2.12:runOpenHouse \
  --args="${FILTERS# }"
