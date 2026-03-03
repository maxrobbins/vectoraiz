# =============================================================================
# vectorAIz — Windows Installer
# =============================================================================
# Usage:
#   irm https://raw.githubusercontent.com/aidotmarket/vectoraiz/main/installers/windows/install-vectoraiz.ps1 | iex
#
# Or download and run:
#   Set-ExecutionPolicy Bypass -Scope Process -Force
#   .\install-vectoraiz.ps1
# =============================================================================

$ErrorActionPreference = "Stop"

# --- Configuration ---
$InstallDir = "$env:USERPROFILE\vectoraiz"
$ComposeFile = "docker-compose.customer.yml"
$ComposeUrl = "https://raw.githubusercontent.com/aidotmarket/vectoraiz/main/docker-compose.customer.yml"
$PreferredPorts = @(80, 8080, 3000, 8888, 9000)

# --- Helpers ---
function Write-Banner {
    Write-Host ""
    Write-Host "  ╔═══════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "  ║                                           ║" -ForegroundColor Cyan
    Write-Host "  ║       ⚡ vectorAIz Installer ⚡           ║" -ForegroundColor Cyan
    Write-Host "  ║                                           ║" -ForegroundColor Cyan
    Write-Host "  ║   Self-hosted data processing & search    ║" -ForegroundColor Cyan
    Write-Host "  ║                 Windows                   ║" -ForegroundColor Cyan
    Write-Host "  ║                                           ║" -ForegroundColor Cyan
    Write-Host "  ╚═══════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Ready {
    param([string]$Url)
    Write-Host ""
    Write-Host "  ╔═══════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "  ║                                           ║" -ForegroundColor Green
    Write-Host "  ║       ✅ vectorAIz is Installed!          ║" -ForegroundColor Green
    Write-Host "  ║                                           ║" -ForegroundColor Green
    Write-Host "  ║   Open your browser to:                   ║" -ForegroundColor Green
    Write-Host "  ║                                           ║" -ForegroundColor Green
    $padding = " " * [Math]::Max(0, 25 - $Url.Length)
    Write-Host "  ║   ➜  $Url$padding║" -ForegroundColor Cyan
    Write-Host "  ║                                           ║" -ForegroundColor Green
    Write-Host "  ║   Shortcuts added to Desktop & Start Menu ║" -ForegroundColor Green
    Write-Host "  ║                                           ║" -ForegroundColor Green
    Write-Host "  ╚═══════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host ""
}

function Write-Fail {
    param([string]$Message)
    Write-Host ""
    Write-Host "  ERROR: $Message" -ForegroundColor Red
    Write-Host ""
    exit 1
}

function Write-Info {
    param([string]$Message)
    Write-Host "  ▸ $Message" -ForegroundColor Blue
}

function Write-Success {
    param([string]$Message)
    Write-Host "  ✓ $Message" -ForegroundColor Green
}

function Write-Warn {
    param([string]$Message)
    Write-Host "  ⚠ $Message" -ForegroundColor Yellow
}

function Get-RandomSecret {
    param([int]$Length = 32)
    $bytes = New-Object byte[] ($Length / 2)
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    return ($bytes | ForEach-Object { $_.ToString("x2") }) -join ""
}

function Test-PortFree {
    param([int]$Port)
    try {
        $listener = New-Object System.Net.Sockets.TcpClient
        $listener.Connect("127.0.0.1", $Port)
        $listener.Close()
        return $false
    } catch {
        return $true
    }
}

function Get-Url {
    param([int]$Port)
    if ($Port -eq 80) { return "http://localhost" }
    return "http://localhost:$Port"
}

# =============================================================================
# Main
# =============================================================================
Write-Banner

# ─── Step 1: Check if running as Administrator ──────────────────
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Warn "Not running as Administrator. Some features may require elevation."
    Write-Host "  To run as Admin: Right-click PowerShell → 'Run as Administrator'" -ForegroundColor DarkGray
    Write-Host ""
}

# ─── Step 2: Check / Install Docker ─────────────────────────────
Write-Info "Checking for Docker..."

$dockerAvailable = $false
try {
    $null = docker info 2>&1
    if ($LASTEXITCODE -eq 0) { $dockerAvailable = $true }
} catch {}

if ($dockerAvailable) {
    Write-Success "Docker is running"
} else {
    # Check if Docker Desktop is installed but not running
    $dockerExe = Get-Command docker -ErrorAction SilentlyContinue
    $dockerDesktopPath = "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
    $dockerInstalled = ($null -ne $dockerExe) -or (Test-Path $dockerDesktopPath)

    if ($dockerInstalled) {
        Write-Warn "Docker is installed but not running."
        Write-Info "Starting Docker Desktop..."

        if (Test-Path $dockerDesktopPath) {
            Start-Process $dockerDesktopPath
        }

        Write-Info "Waiting for Docker to start..."
        $waited = 0
        $maxWait = 120
        while ($waited -lt $maxWait) {
            try {
                $null = docker info 2>&1
                if ($LASTEXITCODE -eq 0) {
                    $dockerAvailable = $true
                    break
                }
            } catch {}
            Start-Sleep -Seconds 3
            $waited += 3
            Write-Host "`r  ⏳ Waiting for Docker daemon... ($($waited)s)" -NoNewline -ForegroundColor Blue
        }
        Write-Host "`r                                                          `r" -NoNewline

        if (-not $dockerAvailable) {
            Write-Fail "Docker did not start within $($maxWait)s. Please start Docker Desktop manually and re-run this installer."
        }
        Write-Success "Docker is running"
    } else {
        Write-Warn "Docker Desktop is not installed."
        Write-Host ""
        Write-Host "  Docker Desktop is required to run vectorAIz." -ForegroundColor White
        Write-Host ""

        $installDocker = Read-Host "  Would you like to download Docker Desktop now? [Y/n]"
        if ($installDocker -ne "n" -and $installDocker -ne "N") {
            Write-Info "Downloading Docker Desktop installer..."
            $dockerInstaller = "$env:TEMP\DockerDesktopInstaller.exe"
            try {
                Invoke-WebRequest -Uri "https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe" -OutFile $dockerInstaller -UseBasicParsing
                Write-Success "Downloaded Docker Desktop installer"

                Write-Info "Running Docker Desktop installer..."
                Write-Host "  Please follow the Docker Desktop installation wizard." -ForegroundColor DarkGray
                Start-Process -FilePath $dockerInstaller -Wait

                Write-Info "Starting Docker Desktop..."
                if (Test-Path "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe") {
                    Start-Process "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
                }

                Write-Info "Waiting for Docker to be ready (first start may take a few minutes)..."
                $waited = 0
                $maxWait = 180
                while ($waited -lt $maxWait) {
                    try {
                        $null = docker info 2>&1
                        if ($LASTEXITCODE -eq 0) {
                            $dockerAvailable = $true
                            break
                        }
                    } catch {}
                    Start-Sleep -Seconds 3
                    $waited += 3
                    Write-Host "`r  ⏳ Waiting for Docker daemon... ($($waited)s)" -NoNewline -ForegroundColor Blue
                }
                Write-Host "`r                                                          `r" -NoNewline

                if (-not $dockerAvailable) {
                    Write-Fail "Docker did not start within $($maxWait)s.`n  Please open Docker Desktop and re-run this installer."
                }
                Write-Success "Docker is installed and running"
            } catch {
                Write-Fail "Failed to download Docker Desktop.`n  Please download manually from: https://docs.docker.com/desktop/install/windows-install/"
            } finally {
                Remove-Item $dockerInstaller -ErrorAction SilentlyContinue
            }
        } else {
            Write-Host ""
            Write-Host "  Download Docker Desktop from:" -ForegroundColor White
            Write-Host "  https://docs.docker.com/desktop/install/windows-install/" -ForegroundColor Cyan
            Write-Host ""
            Write-Host "  After installing Docker, re-run this installer." -ForegroundColor White
            exit 1
        }
    }
}

# ─── Step 3: Create install directory ────────────────────────────
Write-Info "Setting up install directory..."
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
Write-Success "Install directory: $InstallDir"

# ─── Step 4: Download compose file ──────────────────────────────
Write-Info "Downloading docker-compose configuration..."
try {
    Invoke-WebRequest -Uri $ComposeUrl -OutFile "$InstallDir\$ComposeFile" -UseBasicParsing
    Write-Success "Downloaded $ComposeFile"
} catch {
    Write-Fail "Failed to download compose file from GitHub.`n  Check your internet connection and try again."
}

# ─── Step 5: Find available port ─────────────────────────────────
Write-Info "Finding available port..."
$Port = $null

# Check existing .env
$envFile = "$InstallDir\.env"
if (Test-Path $envFile) {
    $existingPort = (Get-Content $envFile | Where-Object { $_ -match "^VECTORAIZ_PORT=" }) -replace "^VECTORAIZ_PORT=", ""
    if ($existingPort -and (Test-PortFree -Port ([int]$existingPort))) {
        $Port = [int]$existingPort
    }
}

if (-not $Port) {
    foreach ($tryPort in $PreferredPorts) {
        if (Test-PortFree -Port $tryPort) {
            $Port = $tryPort
            break
        }
    }
}

if (-not $Port) { $Port = 8080 }

Write-Success "Using port $Port"
$Url = Get-Url -Port $Port


# --- Step 5a: Connected mode prompt ---------------------------------
$VectoraizMode = "standalone"
if (-not (Test-Path $envFile)) {
    $isInteractive = [Environment]::UserInteractive -and (-not $env:CI)
    if (-not $isInteractive) {
        # Non-interactive mode (e.g. CI)
        if ($env:VECTORAIZ_MODE) {
            $VectoraizMode = $env:VECTORAIZ_MODE
            Write-Success "Using VECTORAIZ_MODE=$VectoraizMode from environment"
        } else {
            $VectoraizMode = "standalone"
            Write-Info "Non-interactive install detected, defaulting to standalone mode. Set VECTORAIZ_MODE=connected to enable ai.market features."
        }
    } else {
        Write-Host ""
        Write-Host "  ┌─────────────────────────────────────────────────────────┐"
        Write-Host "  │  Would you like to run vectorAIz in Connected mode?    │"
        Write-Host "  │                                                         │"
        Write-Host "  │  YES — Enables allAI, your AI data assistant            │"
        Write-Host "  │  NO  — Standalone mode, no internet access required     │"
        Write-Host "  └─────────────────────────────────────────────────────────┘"
        Write-Host ""
        do {
            $yn = Read-Host "  Connect to ai.market for AI features? (Y/N)"
            switch -Regex ($yn) {
                "^[Yy]" { $VectoraizMode = "connected"; Write-Success "Connected mode selected — allAI will be available"; break }
                "^[Nn]" { $VectoraizMode = "standalone"; Write-Success "Standalone mode selected"; break }
                default  { Write-Host "  Please answer Y or N." }
            }
        } while ($yn -notmatch "^[YyNn]")
    }
}

# ─── Step 6: Generate .env ───────────────────────────────────────
if (-not (Test-Path $envFile)) {
    Write-Info "Generating secure configuration..."
    $envContent = @"
# vectorAIz Configuration
# Generated on $(Get-Date -Format "yyyy-MM-dd HH:mm:ss UTC")
# Install directory: $InstallDir

# Database password (auto-generated, keep this safe)
POSTGRES_PASSWORD=$(Get-RandomSecret)

# Application secrets
VECTORAIZ_SECRET_KEY=$(Get-RandomSecret)
VECTORAIZ_APIKEY_HMAC_SECRET=$(Get-RandomSecret)

# Port to serve on
VECTORAIZ_PORT=$Port

# Mode: standalone or connected (with allAI)
VECTORAIZ_MODE=$VectoraizMode
"@
    Set-Content -Path $envFile -Value $envContent -Encoding UTF8
    Write-Success "Generated .env with secure defaults"
} else {
    # Update port in existing .env
    $content = Get-Content $envFile -Raw
    if ($content -match "VECTORAIZ_PORT=") {
        $content = $content -replace "VECTORAIZ_PORT=.*", "VECTORAIZ_PORT=$Port"
    } else {
        $content += "`nVECTORAIZ_PORT=$Port"
    }
    Set-Content -Path $envFile -Value $content -Encoding UTF8
    Write-Success "Using existing .env (port updated to $Port)"
}

# ─── Step 7: Pull images ─────────────────────────────────────────
Write-Info "Pulling Docker images (this may take a few minutes)..."
Write-Host ""
Set-Location $InstallDir
docker compose -f $ComposeFile pull
Write-Host ""
Write-Success "All images pulled"

# ─── Step 8: Start containers ────────────────────────────────────
Write-Info "Starting vectorAIz..."
docker compose -f $ComposeFile up -d

# ─── Step 9: Wait for health check ───────────────────────────────
Write-Info "Waiting for vectorAIz to be ready..."
$waited = 0
$maxWait = 180
$healthy = $false
while ($waited -lt $maxWait) {
    try {
        $response = Invoke-WebRequest -Uri "$Url/api/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            $healthy = $true
            break
        }
    } catch {}
    Start-Sleep -Seconds 3
    $waited += 3
    Write-Host "`r  ⏳ Waiting for services to initialize... ($($waited)s)" -NoNewline -ForegroundColor Blue
}
Write-Host "`r                                                          `r" -NoNewline

if ($healthy) {
    Write-Success "All services healthy"
} else {
    Write-Warn "Timed out waiting for health check."
    Write-Host "  The app may still be starting. Try opening $Url in a minute." -ForegroundColor DarkGray
}

# ─── Step 10: Create shortcuts ───────────────────────────────────
Write-Info "Creating shortcuts..."

# Create a launcher script
$launcherScript = @"
# vectorAIz Launcher
`$InstallDir = "$InstallDir"
`$ComposeFile = "$ComposeFile"
`$Port = $Port
`$Url = "$Url"

Set-Location `$InstallDir

# Start Docker Desktop if not running
try {
    `$null = docker info 2>&1
    if (`$LASTEXITCODE -ne 0) { throw }
} catch {
    `$dockerDesktop = "`$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
    if (Test-Path `$dockerDesktop) { Start-Process `$dockerDesktop }
    `$maxWait = 60
    `$waited = 0
    while (`$waited -lt `$maxWait) {
        try { `$null = docker info 2>&1; if (`$LASTEXITCODE -eq 0) { break } } catch {}
        Start-Sleep -Seconds 3
        `$waited += 3
    }
}

docker compose -f `$ComposeFile up -d 2>&1 | Out-Null

# Wait for health then open browser
for (`$i = 0; `$i -lt 40; `$i++) {
    try {
        `$r = Invoke-WebRequest -Uri "`$Url/api/health" -UseBasicParsing -TimeoutSec 2 -ErrorAction SilentlyContinue
        if (`$r.StatusCode -eq 200) { break }
    } catch {}
    Start-Sleep -Seconds 3
}

Start-Process `$Url
"@

$launcherPath = "$InstallDir\launch-vectoraiz.ps1"
Set-Content -Path $launcherPath -Value $launcherScript -Encoding UTF8

# Create Desktop shortcut
try {
    $WshShell = New-Object -ComObject WScript.Shell
    $desktopPath = [Environment]::GetFolderPath("Desktop")
    $shortcut = $WshShell.CreateShortcut("$desktopPath\vectorAIz.lnk")
    $shortcut.TargetPath = "powershell.exe"
    $shortcut.Arguments = "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$launcherPath`""
    $shortcut.WorkingDirectory = $InstallDir
    $shortcut.Description = "vectorAIz — Self-hosted data processing & search"
    $shortcut.Save()
    Write-Success "Desktop shortcut created"
} catch {
    Write-Warn "Could not create desktop shortcut"
}

# Create Start Menu shortcut
try {
    $startMenuPath = "$env:APPDATA\Microsoft\Windows\Start Menu\Programs"
    $shortcut = $WshShell.CreateShortcut("$startMenuPath\vectorAIz.lnk")
    $shortcut.TargetPath = "powershell.exe"
    $shortcut.Arguments = "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$launcherPath`""
    $shortcut.WorkingDirectory = $InstallDir
    $shortcut.Description = "vectorAIz — Self-hosted data processing & search"
    $shortcut.Save()
    Write-Success "Start Menu shortcut created"
} catch {
    Write-Warn "Could not create Start Menu shortcut"
}

# ─── Step 11: Open browser ───────────────────────────────────────
Write-Ready -Url $Url

Start-Sleep -Seconds 1
Start-Process $Url

Write-Host "  Tip: Launch vectorAIz anytime from the Desktop or Start Menu shortcut" -ForegroundColor Cyan
Write-Host "  Tip: View logs: cd $InstallDir; docker compose -f $ComposeFile logs -f" -ForegroundColor Cyan
Write-Host "  Tip: Stop: cd $InstallDir; docker compose -f $ComposeFile down" -ForegroundColor Cyan
Write-Host ""
