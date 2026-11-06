#!/usr/bin/env bash
# Run one synthetic scenario end-to-end through the real pipeline, then snapshot the output.
# macOS/Linux counterpart of run_scenario.ps1. Only needs Docker: the generator runs in a JDK 17
# container on the compose network, so no host JDK is required.
#   cancel jobs -> clear Kafka topic -> ensure schema + truncate DB -> generate -> submit -> wait -> snapshot
# Usage: evaluation/run_scenario.sh <scenario> [--seed N] [--snapshot NAME] [--stable-seconds N] [--max-wait-minutes N]
# Expects the stack to be started already (docker compose up -d) and the jars to be built.
set -euo pipefail

usage() { sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//'; }

SCENARIO=""; SEED=42; SNAPSHOT=""; STABLE_SECONDS=90; MAX_WAIT_MINUTES=25
while [ $# -gt 0 ]; do
  case "$1" in
    --seed) SEED="$2"; shift 2 ;;
    --snapshot) SNAPSHOT="$2"; shift 2 ;;
    --stable-seconds) STABLE_SECONDS="$2"; shift 2 ;;
    --max-wait-minutes) MAX_WAIT_MINUTES="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *) SCENARIO="$1"; shift ;;
  esac
done
[ -n "$SCENARIO" ] || { usage >&2; exit 2; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SNAPSHOT="${SNAPSHOT:-$SCENARIO}"
DATA="$ROOT/evaluation/data"
GEN_JAR="generator/target/circadian-generator.jar"
JDK_IMAGE="maven:3.9-eclipse-temurin-17"
TOPIC="vitals-raw"
JM_API="http://localhost:18081"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

[ -f "$ROOT/scenarios/$SCENARIO.yaml" ] || { echo "no such scenario: scenarios/$SCENARIO.yaml" >&2; exit 2; }
[ -f "$ROOT/$GEN_JAR" ] || { echo "missing $GEN_JAR: build first (see README)" >&2; exit 1; }

psql_q() { docker exec circadian-postgres psql -U circadian -d circadian -tAc "$1"; }

wait_for() { # <description> <seconds> <command...>
  local what="$1" limit="$2"; shift 2
  local end=$(( $(date +%s) + limit ))
  until "$@" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$end" ]; then
      echo "timed out waiting for $what. Is the stack up? (docker compose up -d)" >&2
      exit 1
    fi
    sleep 2
  done
}
# TCP, not the socket: during first-boot init Postgres only listens on the socket.
postgres_ready() { docker exec circadian-postgres pg_isready -q -h 127.0.0.1 -U circadian -d circadian; }
kafka_ready() { docker exec circadian-kafka $KAFKA_TOPICS --list; }
flink_ready() { curl -sf "$JM_API/overview" | grep -Eq '"taskmanagers":[1-9]'; }
running_jobs() { curl -sf "$JM_API/jobs" | grep -oE '"id":"[0-9a-f]{32}","status":"RUNNING"' | grep -oE '[0-9a-f]{32}' || true; }
cancel_job() { curl -sf -X PATCH "$JM_API/jobs/$1?mode=cancel" >/dev/null || true; }

echo "===== SCENARIO: $SCENARIO (seed $SEED) ====="
wait_for "Postgres" 120 postgres_ready
wait_for "Kafka" 120 kafka_ready
wait_for "Flink" 120 flink_ready
mkdir -p "$DATA"

echo "[1/6] cancel running jobs"
JOBS="$(running_jobs)"
if [ -n "$JOBS" ]; then
  for job in $JOBS; do echo "   cancel $job"; cancel_job "$job"; done
  sleep 4
fi

echo "[2/6] reset Kafka topic '$TOPIC'"
docker exec circadian-kafka $KAFKA_TOPICS --delete --topic "$TOPIC" >/dev/null 2>&1 || true
for _ in $(seq 1 20); do
  docker exec circadian-kafka $KAFKA_TOPICS --list 2>/dev/null | grep -qx "$TOPIC" || break
  sleep 1
done
docker exec circadian-kafka $KAFKA_TOPICS --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null

echo "[3/6] ensure schema, truncate Postgres tables"
# sql/init only runs when the data volume is first created, so an older or empty volume can lack
# tables. The schema is all IF NOT EXISTS, which makes re-applying it safe.
docker exec -e PGOPTIONS=-cclient_min_messages=warning circadian-postgres \
  psql -q -v ON_ERROR_STOP=1 -U circadian -d circadian -f /docker-entrypoint-initdb.d/01_schema.sql
psql_q "TRUNCATE agg_1min, circadian_metrics, ground_truth_params, ground_truth_events, dead_letter; DELETE FROM experiment_runs;" >/dev/null

echo "[4/6] generate scenario"
GEN_LOG="$DATA/_gen_$SCENARIO.log"
NETWORK="$(docker inspect circadian-kafka --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')"
GEN_START=$(date +%s)
if ! docker run --rm --network "$NETWORK" -v "$ROOT:/src" -w /src "$JDK_IMAGE" \
    java -jar "$GEN_JAR" --scenario "scenarios/$SCENARIO.yaml" --seed "$SEED" \
    --bootstrap kafka:19092 --topic "$TOPIC" \
    --jdbc-url jdbc:postgresql://postgres:5432/circadian > "$GEN_LOG" 2>&1; then
  tail -20 "$GEN_LOG" >&2
  echo "generator failed, full log: $GEN_LOG" >&2
  exit 1
fi
GEN_SECONDS=$(( $(date +%s) - GEN_START ))
grep -E 'Produced|ground truth' "$GEN_LOG" | sed 's/^/   /' || true
echo "   generation ${GEN_SECONDS}s; ground_truth_params=$(psql_q 'SELECT count(*) FROM ground_truth_params')"

echo "[5/6] submit pipeline job"
PROC_START=$(date +%s)
SUB_LOG="$DATA/_submit_$SCENARIO.log"
docker exec circadian-jobmanager flink run -d /jars/circadian-pipeline.jar > "$SUB_LOG" 2>&1 || true
JOB_ID="$(grep -oE '[0-9a-f]{32}' "$SUB_LOG" | head -1 || true)"
if [ -z "$JOB_ID" ]; then
  tail -20 "$SUB_LOG" >&2
  echo "pipeline submission failed, full log: $SUB_LOG" >&2
  exit 1
fi
echo "   jobId=$JOB_ID"

echo "[6/6] wait for metrics to finalize (stable ${STABLE_SECONDS}s)"
prev=-1; stable=0; finalized=0
deadline=$(( $(date +%s) + MAX_WAIT_MINUTES * 60 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  sleep 15
  metrics="$(psql_q 'SELECT count(*) FROM circadian_metrics')"
  agg="$(psql_q 'SELECT count(*) FROM agg_1min')"
  if [ "$metrics" = "$prev" ]; then stable=$((stable + 15)); else stable=0; fi
  echo "   agg=$agg metrics=$metrics stable=${stable}s"
  prev="$metrics"
  if [ "$metrics" -gt 0 ] && [ "$stable" -ge "$STABLE_SECONDS" ]; then finalized=1; break; fi
done
PROC_SECONDS=$(( $(date +%s) - PROC_START ))

# Cancel the (now idle) streaming job so the next scenario starts clean. Results stay in Postgres.
cancel_job "$JOB_ID"

if [ "$finalized" -ne 1 ]; then
  echo "metrics did not finalize within ${MAX_WAIT_MINUTES} min (agg=$agg metrics=$metrics)" >&2
  exit 1
fi

PATIENTS="$(psql_q 'SELECT count(DISTINCT patient_id) FROM agg_1min')"
PERF="$DATA/perf.csv"
[ -f "$PERF" ] || echo "scenario,patients,agg_rows,metric_rows,gen_seconds,process_seconds" > "$PERF"
echo "$SCENARIO,$PATIENTS,$agg,$metrics,$GEN_SECONDS,$PROC_SECONDS" >> "$PERF"

EV_PY="$ROOT/evaluation/.venv/bin/python"
if [ -x "$EV_PY" ]; then
  echo "   snapshot -> evaluation/data/$SNAPSHOT/"
  "$EV_PY" "$ROOT/evaluation/run_experiment.py" --snapshot "$SNAPSHOT"
else
  echo "   (skipping snapshot: evaluation/.venv not found)"
fi
echo "===== DONE $SCENARIO: agg=$agg metrics=$metrics gen=${GEN_SECONDS}s proc=${PROC_SECONDS}s ====="
