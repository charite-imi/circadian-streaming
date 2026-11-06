# Exp C load sweep: run a fixed-duration clean scenario at increasing patient counts through the
# real pipeline, recording processing time + throughput to data/loadsweep.csv.
#   7-day sim (CWT active — the bottleneck), rate 0.1 Hz, patients swept.
# Usage:  powershell -File run_loadsweep.ps1 -Patients 1,2,4
param(
  # comma-separated string (NOT [int[]]): with `powershell -File`, `-Patients 1,2,4` binds as the
  # string "1,2,4" and [int] coercion reads commas as thousands separators -> 124. Split it ourselves.
  [string]$Patients = "1,2,4",
  [double]$SimHours = 168,
  [double]$RateHz = 0.1,
  [int]$Seed = 42,
  [int]$StableSeconds = 75,
  [int]$MaxWaitMinutes = 25
)
$ErrorActionPreference = "Stop"
$root   = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$scoopJava = "$env:USERPROFILE\scoop\apps\temurin17-jdk\current\bin\java.exe"
$java   = if ($env:JAVA17_HOME) { "$env:JAVA17_HOME\bin\java.exe" } elseif (Test-Path $scoopJava) { $scoopJava } else { "java" }
$genJar = "$root\generator\target\circadian-generator.jar"
$topic  = "vitals-raw"
$jmApi  = "http://localhost:18081"
$csv    = "$root\evaluation\data\loadsweep.csv"
function PSql($sql) { (docker exec circadian-postgres psql -U circadian -d circadian -tAc $sql).Trim() }

# sql/init only runs when the data volume is first created, so an older or empty volume can lack
# tables. The schema is all IF NOT EXISTS, which makes re-applying it safe.
docker exec -e PGOPTIONS=-cclient_min_messages=warning circadian-postgres psql -q -v ON_ERROR_STOP=1 -U circadian -d circadian -f /docker-entrypoint-initdb.d/01_schema.sql | Out-Null

$patientList = $Patients.Split(',') | ForEach-Object { [int]$_.Trim() }
New-Item -ItemType Directory -Force "$root\evaluation\data" | Out-Null
"patients,produced,gen_seconds,process_seconds,agg_rows,metric_rows" | Out-File $csv -Encoding ascii

foreach ($N in $patientList) {
  Write-Host "===== LOAD: $N patients =====" -ForegroundColor Cyan
  # cancel running jobs
  try { (Invoke-RestMethod "$jmApi/jobs").jobs | Where-Object { $_.status -eq "RUNNING" } |
        ForEach-Object { Invoke-RestMethod -Method Patch "$jmApi/jobs/$($_.id)?mode=cancel" | Out-Null }; Start-Sleep 4 } catch {}
  # reset topic
  docker exec circadian-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $topic 2>$null | Out-Null
  for ($i = 0; $i -lt 20; $i++) {
    if (-not (docker exec circadian-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null | Select-String -Quiet "^$topic$")) { break }
    Start-Sleep 1
  }
  docker exec circadian-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 1 --replication-factor 1 2>$null | Out-Null
  PSql "TRUNCATE agg_1min, circadian_metrics, ground_truth_params, ground_truth_events, dead_letter;" | Out-Null

  # generate (clean default: --patients drives load; no scenario file)
  $genLog = "$root\evaluation\data\_gen_load$N.log"
  $genStart = Get-Date
  $prevEA = $ErrorActionPreference; $ErrorActionPreference = "Continue"
  & $java -jar $genJar --patients $N --sim-hours $SimHours --rate-hz $RateHz --seed $Seed `
      --bootstrap localhost:9092 --topic $topic --skip-ground-truth > $genLog 2>&1
  $ErrorActionPreference = $prevEA
  $genSeconds = [math]::Round(((Get-Date) - $genStart).TotalSeconds, 1)
  $produced = [int]([regex]"Produced (\d+) samples").Match((Get-Content $genLog -Raw)).Groups[1].Value

  # submit + time processing
  $procStart = Get-Date
  $subLog = "$root\evaluation\data\_submit_load$N.log"
  $prevEA = $ErrorActionPreference; $ErrorActionPreference = "Continue"
  docker exec circadian-jobmanager flink run -d /jars/circadian-pipeline.jar > $subLog 2>&1
  $ErrorActionPreference = $prevEA
  $jobId = ([regex]"([0-9a-f]{32})").Match((Get-Content $subLog -Raw)).Value

  $prev = -1; $stable = 0; $deadline = (Get-Date).AddMinutes($MaxWaitMinutes)
  while ((Get-Date) -lt $deadline) {
    Start-Sleep 15
    $c = [int](PSql "SELECT count(*) FROM circadian_metrics")
    if ($c -eq $prev) { $stable += 15 } else { $stable = 0 }
    Write-Host ("   metrics={0,-6} stable={1}s" -f $c, $stable)
    $prev = $c
    if ($c -gt 0 -and $stable -ge $StableSeconds) { break }
  }
  $procSeconds = [math]::Round(((Get-Date) - $procStart).TotalSeconds, 1)
  if ($jobId) { try { Invoke-RestMethod -Method Patch "$jmApi/jobs/$jobId?mode=cancel" | Out-Null } catch {} }

  $aggN = [int](PSql "SELECT count(*) FROM agg_1min"); $metN = [int](PSql "SELECT count(*) FROM circadian_metrics")
  "$N,$produced,$genSeconds,$procSeconds,$aggN,$metN" | Out-File $csv -Append -Encoding ascii
  Write-Host "   N=$N produced=$produced proc=${procSeconds}s agg=$aggN metrics=$metN" -ForegroundColor Green
}
Write-Host "LOAD SWEEP DONE -> $csv"
