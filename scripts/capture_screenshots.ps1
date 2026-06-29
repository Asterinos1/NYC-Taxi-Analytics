<#
.SYNOPSIS
    Automates capturing Spark UI, Spark History Server, and Airflow UI screenshots.
.DESCRIPTION
    Uses headless Google Chrome to capture screenshots of active Spark and Airflow UIs 
    and saves them to the docs/screenshots directory for report compilation.
.EXAMPLE
    .\scripts\capture_screenshots.ps1
.LINK
    https://github.com/JuliusBrussee/caveman
#>

$chromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path $chromePath)) {
    Write-Error "Google Chrome not found at $chromePath. Please update the path in this script."
    exit 1
}

$outputDir = Join-Path $PSScriptRoot "../docs/screenshots"
if (-not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

$targets = @(
    @{ Url = "http://localhost:18080"; Name = "spark_history.png" },
    @{ Url = "http://localhost:8080";  Name = "spark_master.png" },
    @{ Url = "http://localhost:8081";  Name = "spark_worker.png" },
    @{ Url = "http://localhost:8082/"; Name = "airflow_home.png" },
    @{ Url = "http://localhost:8082/dags/nyc_taxi_medallion_pipeline/runs"; Name = "airflow_dag_grid.png" }
)

Write-Host "Starting automated Spark UI screenshot capture..." -ForegroundColor Cyan

foreach ($target in $targets) {
    $outputPath = Join-Path $outputDir $target.Name
    Write-Host "Capturing $($target.Url) -> $outputPath" -ForegroundColor Yellow
    
    Start-Process $chromePath -ArgumentList @(
        "--headless",
        "--disable-gpu",
        "--screenshot=$outputPath",
        "--window-size=1280,800",
        "--virtual-time-budget=10000",
        $target.Url
    ) -Wait
}

Write-Host "Screenshots captured successfully in $outputDir" -ForegroundColor Green
