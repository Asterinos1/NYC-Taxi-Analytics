<#
.SYNOPSIS
    Automates starting the pipeline, polling its status, capturing UI screenshots, and harvesting metrics.
.DESCRIPTION
    Stops/starts Docker compose containers (optional), triggers the nyc_taxi_medallion_pipeline DAG,
    polls Airflow for run completion (success/failed), triggers browser screenshot capture,
    and copies metrics.json output if successful.
.PARAMETER FaultInject
    Enables the FAULT_INJECT DAG variable in Airflow to force ValidateJob data quality gate failure.
.PARAMETER SkipComposeUp
    Skips the container health checking and 'docker compose up' command.
.EXAMPLE
    .\scripts\run_and_capture.ps1 -FaultInject
.EXAMPLE
    .\scripts\run_and_capture.ps1 -SkipComposeUp
#>

param(
    [switch]$FaultInject,
    [switch]$SkipComposeUp,
    [switch]$SkipTrigger
)

$ErrorActionPreference = "Continue"

# Resolve paths relative to this script
$rootPath = Resolve-Path (Join-Path $PSScriptRoot "..")
$dockerComposeFile = Join-Path $rootPath "docker/docker-compose.yml"
$screenshotScript = Join-Path $PSScriptRoot "capture_screenshots.ps1"
$docsDir = Join-Path $rootPath "docs"
$screenshotsDir = Join-Path $docsDir "screenshots"
$metricsSource = Join-Path $rootPath "data/gold/metrics.json"

function Capture-PhaseScreenshots($safeRunId, $phaseName) {
    Write-Host "Capturing screenshots for phase '$phaseName'..." -ForegroundColor DarkYellow
    & $screenshotScript

    $phaseDir = Join-Path $docsDir "runs/$safeRunId/$phaseName"
    New-Item -ItemType Directory -Path $phaseDir -Force | Out-Null
    
    $screenshotNames = @("spark_history.png", "spark_master.png", "spark_worker.png", "airflow_home.png", "airflow_dag_grid.png")
    foreach ($name in $screenshotNames) {
        $src = Join-Path $screenshotsDir $name
        $dest = Join-Path $phaseDir $name
        if (Test-Path $src) {
            Copy-Item $src $dest -Force
        }
    }
    Write-Host "Phase '$phaseName' screenshots saved to $phaseDir" -ForegroundColor Gray
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "NYC Taxi Analytics Spark Pipeline Automation & Capture Tool" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Verify Docker Daemon
if (-not $SkipComposeUp) {
    Write-Host "[1/6] Verifying Docker daemon..." -ForegroundColor Yellow
    docker info >$null 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Docker daemon is not running. Please start Docker Desktop and try again."
        exit 1
    }
    Write-Host "Docker daemon is running." -ForegroundColor Green
} else {
    Write-Host "[1/6] Skipping Docker daemon verification (SkipComposeUp set)." -ForegroundColor DarkGray
}

# 2. Start Infrastructure
if (-not $SkipComposeUp) {
    Write-Host "[2/6] Starting docker compose services..." -ForegroundColor Yellow
    docker compose -f $dockerComposeFile up -d
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to start docker compose services."
        exit 1
    }
    Write-Host "Services started successfully." -ForegroundColor Green
} else {
    Write-Host "[2/6] Skipping container start (SkipComposeUp set)." -ForegroundColor DarkGray
}

# 3. Wait for Scheduler to be responsive
Write-Host "[3/6] Waiting for Airflow scheduler to be responsive..." -ForegroundColor Yellow
$schedulerReady = $false
for ($i = 1; $i -le 30; $i++) {
    Write-Host "Ping scheduler attempt $i/30..." -ForegroundColor DarkGray
    $testCmd = docker compose -f $dockerComposeFile exec -T airflow-scheduler airflow dags list 2>&1
    if ($LASTEXITCODE -eq 0) {
        $schedulerReady = $true
        break
    }
    Start-Sleep -Seconds 5
}

if (-not $schedulerReady) {
    Write-Error "Airflow scheduler failed to become responsive after 150 seconds."
    exit 1
}
Write-Host "Scheduler is healthy and responsive." -ForegroundColor Green

# 4. Set FAULT_INJECT Variable
$modeStr = if ($FaultInject) { "true" } else { "false" }
Write-Host "[4/6] Configuring FAULT_INJECT variable to '$modeStr' in Airflow..." -ForegroundColor Yellow
docker compose -f $dockerComposeFile exec -T airflow-scheduler airflow variables set FAULT_INJECT $modeStr >$null
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Failed to set FAULT_INJECT variable. Falling back to OS environment variable."
} else {
    Write-Host "Airflow Variable FAULT_INJECT set to '$modeStr'." -ForegroundColor Green
}

# Restart scheduler + dag-processor so the DAG file is re-parsed with the
# updated FAULT_INJECT value (it controls graph wiring at parse time).
Write-Host "Restarting Airflow scheduler & dag-processor to re-parse DAG with FAULT_INJECT=$modeStr..." -ForegroundColor DarkGray
docker compose -f $dockerComposeFile restart airflow-scheduler airflow-dag-processor 2>$null
Write-Host "Waiting 15 seconds for scheduler to become ready..." -ForegroundColor DarkGray
Start-Sleep -Seconds 15

# 5. Trigger DAG and Poll State
if (-not $SkipTrigger) {
    Write-Host "[5/6] Triggering nyc_taxi_medallion_pipeline..." -ForegroundColor Yellow
    $triggerResult = docker compose -f $dockerComposeFile exec -T airflow-scheduler airflow dags trigger nyc_taxi_medallion_pipeline
    Write-Host $triggerResult -ForegroundColor Gray

    Write-Host "Waiting 5 seconds for run to initialize..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 5
} else {
    Write-Host "[5/6] Polling existing/latest run (SkipTrigger set)..." -ForegroundColor Yellow
}

Write-Host "Polling pipeline state (checks every 15s)..." -ForegroundColor Yellow
$runId = $null
$finalState = $null
$startTime = Get-Date
$Phase1_Captured = $false
$Phase2_Captured = $false

while ($true) {
    $jsonOutput = docker compose -f $dockerComposeFile exec -T airflow-scheduler airflow dags list-runs nyc_taxi_medallion_pipeline -o json 2>$null
    $state = $null
    
    if ($jsonOutput) {
        # Clean any non-JSON prefix/suffix logs
        $jsonLines = $jsonOutput -split "`n" | Where-Object { 
            $_.Trim().StartsWith("[") -or $_.Trim().StartsWith("{") -or $_.Trim().StartsWith("]") -or $_.Trim().StartsWith("}") -or $_.Trim().StartsWith(",") 
        }
        $jsonClean = $jsonLines -join ""
        
        try {
            $runs = ConvertFrom-Json $jsonClean
            if ($runs -and $runs.Count -gt 0) {
                # Find the latest run
                $latestRun = $runs | Sort-Object run_after -Descending | Select-Object -First 1
                $runId = $latestRun.run_id
                $state = $latestRun.state
            }
        } catch {
            # Fallback to plain text matching if JSON parsing fails
        }
    }
    
    # Fallback to text parsing if JSON query failed or returned no items
    if (-not $state) {
        $rawOutput = docker compose -f $dockerComposeFile exec -T airflow-scheduler airflow dags list-runs nyc_taxi_medallion_pipeline 2>$null
        if ($rawOutput) {
            # Parse text lines (e.g. manual__2026-06-29T15:20:00+00:00 | running)
            $lines = $rawOutput -split "`n" | Where-Object { $_ -match "manual__" -or $_ -match "scheduled__" }
            if ($lines -and $lines.Count -gt 0) {
                $latestLine = $lines | Select-Object -First 1
                # Format: run_id | state | execution_date ...
                $parts = $latestLine -split "\|"
                if ($parts.Count -gt 1) {
                    $runId = $parts[0].Trim()
                    $state = $parts[1].Trim()
                }
            }
        }
    }
    
    $elapsed = (Get-Date) - $startTime
    $elapsedStr = [string]::Format("{0:d2}:{1:d2}", $elapsed.Minutes, $elapsed.Seconds)
    
    if ($state) {
        Write-Host "[Time: $elapsedStr] Run: $runId | State: $state" -ForegroundColor Gray
        if ($state -eq "success" -or $state -eq "failed") {
            $finalState = $state
            break
        }
    } else {
        Write-Host "[Time: $elapsedStr] Waiting for run status to appear..." -ForegroundColor DarkGray
    }
    
    # Capture phase screenshots dynamically during the run
    if ($runId -and $state -eq "running") {
        $safeRunId = $runId -replace '[:+]', '-'
        $tasksJson = docker compose -f $dockerComposeFile exec -T airflow-scheduler airflow tasks states-for-dag-run nyc_taxi_medallion_pipeline $runId -o json 2>$null
        if ($tasksJson) {
            $taskLines = $tasksJson -split "`n" | Where-Object { 
                $_.Trim().StartsWith("[") -or $_.Trim().StartsWith("{") -or $_.Trim().StartsWith("]") -or $_.Trim().StartsWith("}") -or $_.Trim().StartsWith(",") 
            }
            $taskClean = $taskLines -join ""
            try {
                $taskInstances = ConvertFrom-Json $taskClean
                
                # Check Phase 1: ingest_bronze or clean_silver is running
                $ingestRunning = $taskInstances | Where-Object { ($_.task_id -eq "ingest_bronze" -or $_.task_id -eq "clean_silver") -and $_.state -eq "running" }
                if ($ingestRunning -and -not $Phase1_Captured) {
                    Capture-PhaseScreenshots $safeRunId "phase1_ingest_clean"
                    $Phase1_Captured = $true
                }
                
                # Check Phase 2: analytics_gold is running
                $analyticsRunning = $taskInstances | Where-Object { $_.task_id -eq "analytics_gold" -and $_.state -eq "running" }
                if ($analyticsRunning -and -not $Phase2_Captured) {
                    Capture-PhaseScreenshots $safeRunId "phase2_analytics"
                    $Phase2_Captured = $true
                }
            } catch {
                # Ignore JSON parse issues
            }
        }
    }
    
    Start-Sleep -Seconds 15
}

Write-Host "Pipeline run finished with state: $finalState" -ForegroundColor $(if ($finalState -eq "success") { "Green" } else { "Red" })

# Create phase 3 completed/failed folder
$safeRunId = if ($runId) { $runId -replace '[:+]', '-' } else { Get-Date -Format "yyyyMMdd_HHmmss" }
$phase3Name = if ($finalState -eq "success") { "phase3_completed" } else { "phase3_failed" }
Capture-PhaseScreenshots $safeRunId $phase3Name

# 6. Capture Screenshots & Harvest Metrics
Write-Host "[6/6] Capturing evidence and harvesting metrics..." -ForegroundColor Yellow

# Ensure directories exist
if (-not (Test-Path $screenshotsDir)) {
    New-Item -ItemType Directory -Path $screenshotsDir -Force | Out-Null
}

# Run the screenshot script
Write-Host "Invoking screenshot script..." -ForegroundColor DarkGray
& $screenshotScript

# Rename files based on run type
$suffix = if ($FaultInject) { "_failed" } else { "_success" }
Write-Host "Renaming screenshots with suffix '$suffix'..." -ForegroundColor DarkGray

$screenshotMappings = @{
    "airflow_dag_grid.png" = "airflow_dag_grid$suffix.png"
    "airflow_home.png"     = "airflow_home$suffix.png"
    "spark_master.png"     = "spark_master$suffix.png"
    "spark_worker.png"     = "spark_worker$suffix.png"
    "spark_history.png"    = "spark_history$suffix.png"
}

foreach ($item in $screenshotMappings.GetEnumerator()) {
    $src = Join-Path $screenshotsDir $item.Key
    $dest = Join-Path $screenshotsDir $item.Value
    if (Test-Path $src) {
        if (Test-Path $dest) { Remove-Item $dest -Force }
        Move-Item $src $dest
        Write-Host "Saved: $($item.Value)" -ForegroundColor Gray
    }
}

# Copy and display metrics if success
if ($finalState -eq "success") {
    if (Test-Path $metricsSource) {
        $destMetrics = Join-Path $docsDir "measurements.json"
        Copy-Item $metricsSource $destMetrics -Force
        Write-Host "Saved metrics artifact to $destMetrics" -ForegroundColor Green
        Write-Host "Metrics summary:" -ForegroundColor Cyan
        Get-Content $destMetrics | Write-Host -ForegroundColor DarkCyan
    } else {
        Write-Warning "Metrics file not found at $metricsSource!"
    }
} elseif ($FaultInject -and (Test-Path $metricsSource)) {
    # If fault-injection failed at ValidateJob, metrics might still be generated
    $destMetrics = Join-Path $docsDir "measurements_failed.json"
    Copy-Item $metricsSource $destMetrics -Force
    Write-Host "Saved fault run metrics to $destMetrics" -ForegroundColor Yellow
}

# Create run-specific bundle
$safeRunId = if ($runId) { $runId -replace '[:+]', '-' } else { Get-Date -Format "yyyyMMdd_HHmmss" }
$runBundleDir = Join-Path $docsDir "runs/$safeRunId"
$runBundleScreenshots = Join-Path $runBundleDir "screenshots"

Write-Host "Bundling run deliverables to $runBundleDir..." -ForegroundColor Yellow
New-Item -ItemType Directory -Path $runBundleScreenshots -Force | Out-Null

# Copy screenshots to bundle (using their original clean names)
foreach ($item in $screenshotMappings.GetEnumerator()) {
    $src = Join-Path $screenshotsDir $item.Value
    $dest = Join-Path $runBundleScreenshots $item.Key
    if (Test-Path $src) {
        Copy-Item $src $dest -Force
    }
}

# Copy metrics to bundle
if (Test-Path $metricsSource) {
    Copy-Item $metricsSource (Join-Path $runBundleDir "measurements.json") -Force
}

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "Automation execution completed!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
