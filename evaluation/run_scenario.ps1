# Run one synthetic scenario end-to-end through the real pipeline, then snapshot the output.
#   cancel job -> clear Kafka topic -> truncate DB -> generate -> submit pipeline -> wait -> snapshot
# Usage:  powershell -File run_scenario.ps1 -Scenario amplitude_decay_long
param(
  [Parameter(Mandatory = $true)][string]$Scenario,
  [int]$Seed = 42,
  [string]$SnapshotName = "",   # snapshot dir name (default = $Scenario); tag multi-seed runs with this
  [int]$StableSeconds = 90,     # require circadian_metrics count unchanged this long => finalized
  [int]$MaxWaitMinutes = 25
)
$ErrorActionPreference = "Stop"
$root   = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$scoopJava = "$env:USERPROFILE\scoop\apps\temurin17-jdk\current\bin\java.exe"
$java   = if ($env:JAVA17_HOME) { "$env:JAVA17_HOME\bin\java.exe" } elseif (Test-Path $scoopJava) { $scoopJava } else { "java" }
$genJar = "$root\generator\target\circadian-generator.jar"
$evPy   = "$root\evaluation\.venv\Scripts\python.exe"
$topic  = "vitals-raw"
$jmApi  = "http://localhost:18081"

function PSql($sql) { (docker exec circadian-postgres psql -U circadian -d circadian -tAc $sql).Trim() }

Write-Host "===== SCENARIO: $Scenario (seed $Seed) =====" -ForegroundColor Cyan

# 1) cancel any running Flink job (REST, avoids CLI parsing) ------------------
Write-Host "[1/6] cancel running jobs"
try {
  $jobs = Invoke-RestMethod "$jmApi/jobs"
  foreach ($j in $jobs.jobs) {
    if ($j.status -eq "RUNNING") {
      Write-Host "   cancel $($j.id)"
      Invoke-RestMethod -Method Patch "$jmApi/jobs/$($j.id)?mode=cancel" | Out-Null
    }
  }
  Start-Sleep -Seconds 4
} catch { Write-Host "   (no jobmanager REST yet: $($_.Exception.Message))" }

# 2) clear Kafka topic (delete + wait gone + recreate) -----------------------
Write-Host "[2/6] reset Kafka topic '$topic'"
docker exec circadian-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $topic 2>$null | Out-Null
for ($i = 0; $i -lt 20; $i++) {
  $exists = docker exec circadian-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null | Select-String -Quiet "^$topic$"
  if (-not $exists) { break }
  Start-Sleep -Seconds 1
}
docker exec circadian-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 1 --replication-factor 1 2>$null | Out-Null

# 3) truncate DB -------------------------------------------------------------
Write-Host "[3/6] ensure schema, truncate Postgres tables"
# sql/init only runs when the data volume is first created, so an older or empty volume can lack
# tables. The schema is all IF NOT EXISTS, which makes re-applying it safe.
docker exec -e PGOPTIONS=-cclient_min_messages=warning circadian-postgres psql -q -v ON_ERROR_STOP=1 -U circadian -d circadian -f /docker-entrypoint-initdb.d/01_schema.sql | Out-Null
PSql "TRUNCATE agg_1min, circadian_metrics, ground_truth_params, ground_truth_events, dead_letter; DELETE FROM experiment_runs;" | Out-Null

# 4) generate (ground truth + stream to Kafka) -------------------------------
# NOTE: slf4j-simple logs to STDERR; in PS5.1 piping native stderr (2>&1 |) throws
# NativeCommandError, so redirect all streams straight to a file instead.
Write-Host "[4/6] generate scenario"
New-Item -ItemType Directory -Force "$root\evaluation\data" | Out-Null
$genLog = "$root\evaluation\data\_gen_$Scenario.log"
$genStart = Get-Date
$prevEA = $ErrorActionPreference; $ErrorActionPreference = "Continue"
& $java -jar $genJar --scenario "$root\scenarios\$Scenario.yaml" --seed $Seed `
    --bootstrap localhost:9092 --topic $topic `
    --jdbc-url jdbc:postgresql://localhost:5545/circadian > $genLog 2>&1
$ErrorActionPreference = $prevEA
$genSeconds = [math]::Round(((Get-Date) - $genStart).TotalSeconds, 1)
Get-Content $genLog | Select-String "Produced|ground truth" | ForEach-Object { Write-Host "   $_" }
$produced = [int](PSql "SELECT count(*) FROM ground_truth_params")   # sanity: gt written
Write-Host "   generation $genSeconds s; ground_truth_params=$produced"

# 5) submit pipeline ---------------------------------------------------------
Write-Host "[5/6] submit pipeline job"
$procStart = Get-Date
$subLog = "$root\evaluation\data\_submit_$Scenario.log"
$prevEA = $ErrorActionPreference; $ErrorActionPreference = "Continue"
docker exec circadian-jobmanager flink run -d /jars/circadian-pipeline.jar > $subLog 2>&1
$ErrorActionPreference = $prevEA
$jobId = ([regex]"([0-9a-f]{32})").Match((Get-Content $subLog -Raw)).Value
Write-Host "   jobId=$jobId"

# 6) wait until metrics finalize, then snapshot ------------------------------
Write-Host "[6/6] wait for metrics to finalize (stable ${StableSeconds}s)"
$prev = -1; $stable = 0
$deadline = (Get-Date).AddMinutes($MaxWaitMinutes)
while ((Get-Date) -lt $deadline) {
  Start-Sleep -Seconds 15
  $c = [int](PSql "SELECT count(*) FROM circadian_metrics")
  $a = [int](PSql "SELECT count(*) FROM agg_1min")
  if ($c -eq $prev) { $stable += 15 } else { $stable = 0 }
  Write-Host ("   agg={0,-7} metrics={1,-6} stable={2}s" -f $a, $c, $stable)
  $prev = $c
  if ($c -gt 0 -and $stable -ge $StableSeconds) { break }
}
$procSeconds = [math]::Round(((Get-Date) - $procStart).TotalSeconds, 1)

# cancel the (now-idle) streaming job so the next scenario starts clean
if ($jobId) { try { Invoke-RestMethod -Method Patch "$jmApi/jobs/$jobId?mode=cancel" | Out-Null } catch {} }

# perf log for Exp C
$aggN = [int](PSql "SELECT count(*) FROM agg_1min"); $metN = [int](PSql "SELECT count(*) FROM circadian_metrics")
$patN = [int](PSql "SELECT count(DISTINCT patient_id) FROM agg_1min")
$perf = "$root\evaluation\data\perf.csv"
if (-not (Test-Path $perf)) { "scenario,patients,agg_rows,metric_rows,gen_seconds,process_seconds" | Out-File $perf -Encoding utf8 }
"$Scenario,$patN,$aggN,$metN,$genSeconds,$procSeconds" | Out-File $perf -Append -Encoding utf8

$snapName = if ($SnapshotName) { $SnapshotName } else { $Scenario }
Write-Host "   snapshot -> data/$snapName/"
& $evPy "$root\evaluation\run_experiment.py" --snapshot $snapName
Write-Host "===== DONE $Scenario : agg=$aggN metrics=$metN gen=${genSeconds}s proc=${procSeconds}s =====" -ForegroundColor Green
