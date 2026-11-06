# Real-Time Circadian Analysis Pipeline

Streaming pipeline that consumes high-frequency ICU vital-sign data, computes circadian-rhythm
metrics (Cosinor + CWT) on sliding windows, persists them to PostgreSQL, and serves them to a
clinician dashboard. Implementation behind a B.Sc. thesis (TU Berlin / Charité, Institute of
Medical Informatics). All data is **synthetic with ground truth**, generated for evaluation.

## Architecture (target)

```
SyntheticSource ─▶ Kafka (vitals-raw) ─▶ [ Flink job ] ─▶ PostgreSQL ─▶ REST API ─▶ Grafana
                                              │
                                   Stage 1  Ingestion & Validation
                                   Stage 2  1-min Tumbling Aggregation
                                   Stage 3a Cosinor (24 h sliding)
                                   Stage 3b CWT (5-day sliding) ─▶ CRI
                                   Stage 4  Sink (Postgres)
```

## Modules

| Module      | Responsibility                                              | Phase 0 state |
|-------------|-------------------------------------------------------------|---------------|
| `common`    | Message schema, domain models, JSON serde (no Flink/Kafka)  | implemented   |
| `analytics` | Pure algorithms: Cosinor, CWT, IS/IV (no Flink/Kafka)       | implemented — Cosinor, Morlet CWT (CRI), IS/IV, window stats; unit-tested against known signals |
| `generator` | Synthetic generator → Kafka producer + ground-truth writer  | scenarios + injectable disruptions + fidelity (correlated noise/artifacts/gaps); writes `ground_truth_params` + `ground_truth_events` |
| `pipeline`  | Flink job: stage wiring + Postgres sink                     | Stages 1–3: validation + dead-letter, 1-min median/SD, sliding Cosinor/CWT/IS-IV → `circadian_metrics` |
| `api`       | Thin REST API over Postgres                                 | light-ceiling demonstrator (Phase 4); REST endpoints pending (Phase 3) |

## Quick start (macOS / Linux)

Needs only **Docker** and **Python 3.11–3.13**. The build and the generator run in a JDK 17 container,
so no host JDK or Maven is required.

```bash
./quickstart.sh
```

This builds the jars, starts the stack, creates the Python environments, runs the `demo_decay`
scenario (2 patients, 14 simulated days) and then serves the dashboard. Pass another scenario with
`./quickstart.sh baseline`, or add `--no-dashboard` to skip serving it. To run further scenarios
against a running stack, use `evaluation/run_scenario.sh <scenario>`.

| Service | Address | Notes |
|---|---|---|
| Custom dashboard | http://localhost:8000 | scalogram, cone of influence, lighting therapy |
| Grafana | http://localhost:3000 | admin/admin; set the time range to the simulated dates (June 2026 by default) |
| Flink Web UI | http://localhost:18081 | throughput, backpressure, checkpoints |
| Kafka | `localhost:9092` | topic `vitals-raw` |
| Postgres | `localhost:5545` | db/user/password `circadian` |

Stop the stack and keep the data with `docker compose down`. Add `-v` to also delete the data.

## Prerequisites (manual setup)

- **JDK 17** (Flink 2.2 does not support newer JDKs). Point `JAVA_HOME` at it to build/run; the
  system default JDK is left untouched. (The helper scripts auto-detect a scoop `temurin17-jdk` or
  honour `JAVA17_HOME`.)
- **Maven 3.9+**.
- **Docker** (Kafka + Postgres + Grafana via Compose).

## Manual setup (Windows PowerShell)

`evaluation\run_scenario.ps1 -Scenario <scenario>` runs steps 3–4 for you, including the topic and
database reset. It needs `evaluation\.venv` for its snapshot step.

```powershell
# 1. Infra + Flink cluster (Kafka :9092, Postgres :5545, Grafana :3000, Flink Web UI :18081)
docker compose up -d

# 2. Build everything (requires JDK 17 + Maven 3.9+)
mvn clean package

# 3. Generate from a scenario (ground truth -> Postgres, samples -> Kafka; deterministic via --seed)
java -jar generator/target/circadian-generator.jar --scenario scenarios/amplitude_decay.yaml --seed 42
#    or a quick ad-hoc clean cohort:  java -jar generator/target/circadian-generator.jar --patients 3 --sim-hours 3

# 4. Submit the pipeline to the Flink cluster (runs on the cluster, not this shell).
docker compose exec jobmanager flink run -d /jars/circadian-pipeline.jar
#    Watch it: Flink Web UI at http://localhost:18081 (job graph, checkpoints, backpressure, watermarks).
#    Cancel it there, or:  docker compose exec jobmanager flink cancel <jobId>
```

> The generator and API run on the host and reach Kafka/Postgres at `localhost:9092` / `localhost:5545`.
> The **pipeline runs inside the cluster** and reaches them as `kafka:19092` / `postgres:5432` (the
> compose network). It is no longer run via `java -jar` — Flink core is `provided` by the cluster, so
> the job jar is a slim artifact submitted with `flink run`.

## Dashboard

After `docker compose up -d` and a pipeline run, open **http://localhost:3000** (anonymous viewing
is enabled) → folder **Circadian** → **Patient Detail** (`/d/circadian-detail`). Choose a patient and
signal to see the 1-min median, the Cosinor amplitude (it falls when a disruption is injected), the
CWT CRI, R², IS/IV, and a table of the ground-truth disruptions. Dashboards are provisioned as code
under `grafana/` (datasource + dashboard JSON), so they come up with the stack.

## Light ceiling

A demonstrator that turns a patient's circadian metrics into a 24-hour Dynamic Lighting Therapy
schedule (intensity + colour temperature) and renders a self-contained HTML "light ceiling":

```powershell
java -cp api/target/circadian-api.jar de.tuberlin.circadian.api.lightceiling.LightCeilingDemo --signal HR --out light-ceiling.html
```

Open the generated `light-ceiling.html`. It reads each patient's latest Cosinor acrophase + CWT CRI
from Postgres; pass `--examples` to render three illustrative patients without the database. The DLT
model itself lives in `analytics` (`DltSchedule`, unit-tested); the bright block centres on the
patient's rhythm peak and a weaker rhythm gets a higher-contrast schedule.

## Scenarios

YAML files in `scenarios/` define deterministic experiment configs (cohort size, duration, rate,
signal fidelity, and a disruption schedule). Pass one with `--scenario`; `--seed` is always honoured.

| Scenario | Purpose |
|---|---|
| `baseline.yaml` | Stable rhythms, no disruptions (Exp A correctness) |
| `amplitude_decay.yaml` | Gradual amplitude flattening (the non-stationary case for CWT, Exp B) |
| `phase_shift.yaml` | Gradual acrophase shift (Exp B) |
| `fragmentation.yaml` | Rhythm breaks into short bouts (Exp B) |
| `cohort.yaml` | Mixed preserved/disrupted cohort (Exp D / RQ3) |
| `amplitude_decay_long.yaml` · `phase_shift_long.yaml` · `fragmentation_long.yaml` | 14 days, day-7 onset — sized for the **5-day CWT window** (Exp B detection latency; run with the default `CWT_DAYS`) |
| `smoke.yaml` · `verify_p2.yaml` | Small scenarios for a fast end-to-end / Phase-2 check |

> The short disruption scenarios (48–72 h) can't fill the default 5-day CWT window, so they emit no
> CWT rows unless you reduce `CWT_DAYS`; the `*_long` scenarios are sized for the default window.

## Configuration

- **Generator:** CLI flags (`--patients`, `--seed`, `--sim-hours`, `--rate-hz`, `--bootstrap`, `--jdbc-url`, …). Run with `--help` for the full list.
- **Pipeline:** environment variables. Infra/Stage 1-2: `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`,
  `KAFKA_GROUP`, `JDBC_URL`, `JDBC_USER`, `JDBC_PASSWORD`, `WINDOW_SECONDS`,
  `EXPECTED_SAMPLES_PER_MIN`, `PARALLELISM`, `MAX_OUT_OF_ORDERNESS_SECONDS`. Stage 3 (the
  detection-latency knobs): `COSINOR_HOURS` (24), `CWT_DAYS` (5), `HOP_HOURS` (1), `CWT_OMEGA0` (6),
  `CWT_VOICES` (8), `CWT_MIN_PERIOD_HOURS` (0.5), `CWT_MAX_PERIOD_HOURS` (48), `BAND_LOW_HOURS` (20),
  `BAND_HIGH_HOURS` (28), `COSINOR_MIN_COVERAGE` (0.5), `CWT_MIN_COVERAGE` (0.7), `ENABLE_ISIV` (true).
  Fault tolerance / operability: `CHECKPOINT_INTERVAL_MS` (30000; `0` disables checkpointing),
  `CHECKPOINT_DIR` (a `file://` URI; defaults under the system temp dir), `STATE_BACKEND`
  (`rocksdb` | `hashmap`), `RESTART_ATTEMPTS` (3), `RESTART_DELAY_SECONDS` (10), `IDLE_SECONDS` (60),
  `JDBC_BATCH_SIZE` (500).