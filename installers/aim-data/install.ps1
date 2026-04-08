$ErrorActionPreference = "Stop"

$InstallDir = if ($env:AIM_DATA_INSTALL_DIR) { $env:AIM_DATA_INSTALL_DIR } else { Join-Path $HOME "aim-data" }
$ComposeFile = "docker-compose.aim-data.yml"
$ComposeUrl = "https://raw.githubusercontent.com/aidotmarket/vectoraiz/main/docker-compose.aim-data.yml"
$Image = "ghcr.io/aidotmarket/aim-data:latest"

function Write-Banner {
    Write-Host ""
    Write-Host "  ⚡ AIM-Data Installer" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Info {
    param([string]$Message)
    Write-Host "  ▸ $Message" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "  ✔ $Message" -ForegroundColor Green
}

function Write-Warn {
    param([string]$Message)
    Write-Host "  ! $Message" -ForegroundColor Yellow
}

function Write-Fail {
    param([string]$Message)
    Write-Host ""
    Write-Host "  ✘ $Message" -ForegroundColor Red
    Write-Host ""
    exit 1
}

function New-RandomSecret {
    param([int]$Length = 32)
    $bytes = New-Object byte[] ($Length / 2)
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    return ($bytes | ForEach-Object { $_.ToString("x2") }) -join ""
}

Write-Banner

try {
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) { throw }
} catch {
    $dockerDesktop = Join-Path $env:ProgramFiles "Docker\Docker\Docker Desktop.exe"
    if (Test-Path $dockerDesktop) {
        Write-Fail "Docker Desktop is installed but not running. Start Docker Desktop, then re-run this installer."
    }
    Write-Fail "Docker Desktop is not installed. Install it from https://docs.docker.com/desktop/setup/install/windows-install/ and re-run."
}
Write-Success "Docker is ready"

New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
Set-Location $InstallDir
Write-Success "Install dir: $InstallDir"

Write-Info "Downloading $ComposeFile..."
try {
    Invoke-WebRequest -Uri $ComposeUrl -OutFile (Join-Path $InstallDir $ComposeFile) -UseBasicParsing
} catch {
    Write-Fail "Failed to download compose file from GitHub."
}
Write-Success "Downloaded compose file"

$envFile = Join-Path $InstallDir ".env"
if (-not (Test-Path $envFile)) {
    $envContent = @"
# AIM-Data configuration
POSTGRES_PASSWORD=$(New-RandomSecret)
AIM_DATA_SECRET_KEY=$(New-RandomSecret)
AIM_DATA_VERSION=latest
AIM_DATA_PORT=8080
AIM_DATA_MODE=standalone
"@
    Set-Content -Path $envFile -Value $envContent -Encoding UTF8
    Write-Success "Generated .env"
} else {
    Write-Info ".env already exists - keeping it"
}

Write-Info "Pulling $Image..."
docker pull $Image
if ($LASTEXITCODE -ne 0) {
    Write-Fail "Failed to pull $Image"
}
Write-Success "Image pulled"

Write-Info "Starting AIM-Data..."
docker compose -f $ComposeFile up -d
if ($LASTEXITCODE -ne 0) {
    Write-Fail "docker compose up failed"
}
Write-Success "Containers started"

$Port = 8080
try {
    $portLine = Get-Content $envFile | Where-Object { $_ -match '^AIM_DATA_PORT=' } | Select-Object -First 1
    if ($portLine) {
        $Port = [int](($portLine -replace '^AIM_DATA_PORT=', '').Trim())
    }
} catch {}

$Url = "http://localhost:$Port"

Write-Host ""
Write-Host "  ✅ AIM-Data is running" -ForegroundColor Green
Write-Host "     URL:   $Url" -ForegroundColor Cyan
Write-Host "     Dir:   $InstallDir"
Write-Host "     Logs:  docker compose -f $InstallDir\\$ComposeFile logs -f"
Write-Host ""
