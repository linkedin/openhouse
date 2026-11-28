#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(git rev-parse --show-toplevel)"
RECIPE="$ROOT/infra/recipes/docker-compose/oh-multicluster"
PROJECT="${CASCADE_SMOKE_PROJECT:-oh-multicluster-cascade-smoke}"
PORT_A="${CASCADE_SMOKE_PORT_A:-18000}"
PORT_B="${CASCADE_SMOKE_PORT_B:-18010}"
TOKEN_DIR="$(mktemp -d)"
export CASCADE_SMOKE_PORT_A="$PORT_A"
export CASCADE_SMOKE_PORT_B="$PORT_B"
export CASCADE_SMOKE_URL_A="http://localhost:$PORT_A"
export CASCADE_SMOKE_URL_B="http://localhost:$PORT_B"
export OPENHOUSE_REPLICATION_CASCADE_KEY="${OPENHOUSE_REPLICATION_CASCADE_KEY:-local-only-multicluster-replication-signing-key}"
export CASCADE_SMOKE_TOKEN_OPENHOUSE="$TOKEN_DIR/openhouse.token"
export CASCADE_SMOKE_TOKEN_U_TABLEOWNER="$TOKEN_DIR/u_tableowner.token"

if [[ ! "$PROJECT" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]*$ ]]; then
  echo "CASCADE_SMOKE_PROJECT must contain only letters, digits, underscores, and hyphens." >&2
  exit 2
fi
for port in "$PORT_A" "$PORT_B"; do
  if [[ ! "$port" =~ ^[0-9]+$ ]] || ((port < 1 || port > 65535)); then
    echo "Cascade smoke test ports must be integers from 1 through 65535." >&2
    exit 2
  fi
done

compose() {
  docker compose \
    --project-name "$PROJECT" \
    --file "$RECIPE/docker-compose.yml" \
    --file "$RECIPE/docker-compose.cascade-smoke.yml" \
    "$@"
}

cleanup() {
  compose down
  rm -rf "$TOKEN_DIR"
}
trap cleanup EXIT

cd "$ROOT"
./gradlew \
  :services:tables:bootJar \
  :services:housetables:bootJar \
  :scripts:java:tools:dummytokens:jar \
  -x CopyGitHooksTask
java -jar "$ROOT/build/dummytokens/libs/dummytokens.jar" -d "$TOKEN_DIR"

compose up -d --build tables-a tables-b

ready=false
for _ in $(seq 1 60); do
  status_a="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 3 "$CASCADE_SMOKE_URL_A/v1/databases" || true)"
  status_b="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 3 "$CASCADE_SMOKE_URL_B/v1/databases" || true)"
  if [[ "$status_a" == 401 && "$status_b" == 401 ]]; then
    ready=true
    break
  fi
  sleep 5
done
if [[ "$ready" != true ]]; then
  compose ps
  compose logs --tail=100 tables-a tables-b
  echo "Tables APIs did not become ready within five minutes." >&2
  exit 1
fi

for namenode in namenode-a namenode-b; do
  compose exec -T "$namenode" hdfs dfs -mkdir -p /data/openhouse
  compose exec -T "$namenode" hdfs dfs -chmod -R 777 /data/openhouse
done

python3 "$RECIPE/cascade_smoke_test.py"
