# Tier-2 Exp B: run the *_long detection-latency scenarios at several seeds, each snapshotted to a
# seed-tagged dir data/<scenario>_s<seed>/ (the canonical seed-42 dirs are left untouched).
# Idempotent: a scenario/seed whose snapshot already exists is skipped.
# Run it with the call operator, NOT `powershell -File` (nested -File mangles the loop):
#     powershell -ExecutionPolicy Bypass -Command "& '.\run_multiseed.ps1'"
param(
  [int[]]$Seeds = @(43, 44, 45, 46),
  [string[]]$Scenarios = @('amplitude_decay_long', 'phase_shift_long', 'fragmentation_long'),
  [int]$StableSeconds = 90,
  [int]$MaxWaitMinutes = 25
)
Set-ExecutionPolicy Bypass -Scope Process -Force
$base = "E:\IT Projects\BachelorStreams\project\evaluation"

foreach ($seed in $Seeds) {
  foreach ($scen in $Scenarios) {
    $snap = "${scen}_s${seed}"
    if (Test-Path "$base\data\$snap\circadian_metrics.csv") { Write-Host "skip $snap (exists)"; continue }
    Write-Host "=== $snap ===" -ForegroundColor Cyan
    & "$base\run_scenario.ps1" -Scenario $scen -Seed $seed -SnapshotName $snap `
        -StableSeconds $StableSeconds -MaxWaitMinutes $MaxWaitMinutes *> "$base\data\_ms_${snap}.log"
    (Get-Content "$base\data\_ms_${snap}.log" | Select-String 'DONE').Line
  }
}
Write-Host "MULTISEED DONE" -ForegroundColor Green
