#!/usr/bin/env pwsh
# ETL4 stack bring-up. Idempotent: safe to re-run against a healthy stack.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Compose = Join-Path $PSScriptRoot 'docker-compose.yml'
$Timeout = 60

function Test-Tcp($hostName, $port) {
    $c = [Net.Sockets.TcpClient]::new()
    try {
        $iar = $c.BeginConnect($hostName, $port, $null, $null)
        if ($iar.AsyncWaitHandle.WaitOne(500)) { $c.EndConnect($iar); return $true }
        return $false
    } catch { return $false } finally { $c.Close() }
}

function Test-Http($url) {
    try { (Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2).StatusCode -eq 200 }
    catch { return $false }
}

function Wait-Service([string]$svc, [scriptblock]$check) {
    Write-Host ("  {0,-16} " -f $svc) -NoNewline
    $deadline = (Get-Date).AddSeconds($Timeout)
    while ((Get-Date) -lt $deadline) {
        if (& $check) { Write-Host 'healthy'; return }
        Start-Sleep -Milliseconds 500
    }
    Write-Host 'TIMEOUT'
    docker compose -f $Compose logs --tail=20 $svc
    exit 1
}

# 1. Docker Desktop running?
try { docker info --format '{{.ServerVersion}}' 2>$null | Out-Null }
catch { Write-Host 'Docker is not running - start Docker Desktop and re-run.'; exit 1 }
if ($LASTEXITCODE -ne 0) { Write-Host 'Docker is not running - start Docker Desktop and re-run.'; exit 1 }

# 2. Bring up
Write-Host 'Bringing up the ETL4 stack...'
docker compose -f $Compose up -d | Out-Host

# 3. Health polls
$pw = if ($env:POSTGRES_PASSWORD) { $env:POSTGRES_PASSWORD } else { 'change_me' }
Write-Host "`nWaiting for services (${Timeout}s per service)..."
Wait-Service 'kafka'          { if (-not (Test-Tcp 'localhost' 9092)) { return $false }; docker exec etl4-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list *> $null; return $LASTEXITCODE -eq 0 }
Wait-Service 'postgres'       { if (-not (Test-Tcp 'localhost' 5432)) { return $false }; $out = docker exec -e "PGPASSWORD=$pw" etl4-postgres psql -U etl4 -d etl4 -tAc 'SELECT 1' 2>$null; return ($out -match '1') }
Wait-Service 'prometheus'     { Test-Http 'http://localhost:9090/-/ready' }
Wait-Service 'grafana'        { Test-Http 'http://localhost:3000/api/health' }
Wait-Service 'spark-master'   { Test-Tcp 'localhost' 8080 }
Wait-Service 'spark-worker-1' { Test-Tcp 'localhost' 8081 }
Wait-Service 'spark-worker-2' { (docker inspect -f '{{.State.Running}}' etl4-spark-worker-2 2>$null) -eq 'true' }
Wait-Service 'node-exporter'  { Test-Http 'http://localhost:9100/metrics' }

# 5. DDL (CREATE TABLE IF NOT EXISTS -> safe to re-run)
Write-Host "`nApplying Postgres DDL..."
Get-Content (Join-Path $PSScriptRoot 'create_tables.sql') -Raw | docker exec -e "PGPASSWORD=$pw" -i etl4-postgres psql -U etl4 -d etl4 -v ON_ERROR_STOP=1 | Out-Host

# 6. Topics (--if-not-exists -> safe to re-run)
Write-Host "`nCreating Kafka topics..."
bash (Join-Path $PSScriptRoot 'create_topics.sh') | Out-Host

# 7. Summary
Write-Host "`n--- Stack ready ---"
$svcs = @(
    @{ n = 'Kafka';          u = 'localhost:9092';           p = 'ingest bus (adsb.raw / adsb.dlq)' }
    @{ n = 'Postgres';       u = 'localhost:5432';           p = 'db=etl4 user=etl4 (results tables)' }
    @{ n = 'Prometheus';     u = 'http://localhost:9090';    p = 'metrics scraper' }
    @{ n = 'Grafana';        u = 'http://localhost:3000';    p = 'dashboards - login admin/admin' }
    @{ n = 'Spark master';   u = 'http://localhost:8080';    p = 'cluster UI' }
    @{ n = 'Spark worker-1'; u = 'http://localhost:8081';    p = 'worker UI' }
    @{ n = 'Spark worker-2'; u = '(no external port)';       p = 'worker 2 - internal only' }
    @{ n = 'node-exporter';  u = 'http://localhost:9100';    p = 'host metrics' }
)
foreach ($s in $svcs) { Write-Host ("  {0,-16} {1,-32} {2}" -f $s.n, $s.u, $s.p) }
Write-Host "`nTo tear down:  docker compose -f etl4/infra/docker-compose.yml down  (add -v to wipe Postgres state)"
