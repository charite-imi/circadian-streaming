#!/usr/bin/env bash
# One-command demo for macOS/Linux: build, start the stack, run a synthetic scenario, serve the dashboard.
# Needs Docker and Python 3.11-3.13. The build and generator use a JDK 17 container, so no host JDK or Maven.
# Usage: ./quickstart.sh [scenario] [--seed N] [--no-dashboard]     (default scenario: demo_decay)
set -euo pipefail

usage() { sed -n '2,4p' "$0" | sed 's/^# \{0,1\}//'; }

SCENARIO="demo_decay"; SEED=42; DASHBOARD=1
while [ $# -gt 0 ]; do
  case "$1" in
    --seed) SEED="$2"; shift 2 ;;
    --no-dashboard) DASHBOARD=0; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *) SCENARIO="$1"; shift ;;
  esac
done

cd "$(dirname "${BASH_SOURCE[0]}")"
JDK_IMAGE="maven:3.9-eclipse-temurin-17"

docker info >/dev/null 2>&1 || { echo "Docker is not running. Start Docker Desktop and retry." >&2; exit 1; }

find_python() {
  local candidate
  for candidate in python3.12 python3.13 python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1 &&
       "$candidate" -c 'import sys; sys.exit(0 if (3, 11) <= sys.version_info[:2] <= (3, 13) else 1)'; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

setup_venv() { # <directory>
  [ -x "$1/.venv/bin/python" ] && return 0
  echo "   creating $1/.venv"
  "$PYTHON" -m venv "$1/.venv"
  "$1/.venv/bin/python" -m pip install -q --disable-pip-version-check -r "$1/requirements.txt"
}

PYTHON="$(find_python)" || { echo "Python 3.11-3.13 not found (needed for the dashboard and evaluation)." >&2; exit 1; }

echo "== Build (Maven on JDK 17, in a container)"
docker run --rm -v "$PWD:/src" -w /src -v circadian-m2:/root/.m2 "$JDK_IMAGE" mvn -q -B clean package

echo "== Start Kafka, Postgres, Grafana, Flink"
docker compose up -d

echo "== Python environments"
setup_venv dashboard/backend
setup_venv evaluation

echo "== Run scenario"
evaluation/run_scenario.sh "$SCENARIO" --seed "$SEED"

cat <<'EOF'

Results are ready:
  Dashboard  http://localhost:8000
  Grafana    http://localhost:3000  (folder Circadian; set the time range to the simulated dates)
  Flink UI   http://localhost:18081

Stop the stack, keeping data:  docker compose down
EOF

if [ "$DASHBOARD" -eq 1 ]; then
  echo "Serving the dashboard. Press Ctrl-C to stop it; the stack keeps running."
  exec dashboard/backend/.venv/bin/python -m uvicorn --app-dir dashboard/backend app:app --port 8000
fi
