#!/bin/bash
# =============================================================================
# vectorAIz — macOS Installer
# =============================================================================
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/aidotmarket/vectoraiz/main/installers/mac/install-mac.sh | bash
#
# Or download and run:
#   chmod +x install-mac.sh && ./install-mac.sh
# =============================================================================

set -e

# Ensure common macOS tool paths are available (curl|bash subshells may have minimal PATH)
for p in /usr/local/bin /opt/homebrew/bin; do
    [[ -d "$p" ]] && [[ ":$PATH:" != *":$p:"* ]] && export PATH="$p:$PATH"
done

cd "$HOME" 2>/dev/null || cd /tmp

# --- Configuration ---
INSTALL_DIR="$HOME/vectoraiz"
COMPOSE_FILE="docker-compose.customer.yml"
# --- Versioned compose download (Council S197: no main branch race) ---
INSTALL_REF="${INSTALL_REF:-}"
GITHUB_REPO="aidotmarket/vectoraiz"

if [ -n "$INSTALL_REF" ]; then
    COMPOSE_URL="https://raw.githubusercontent.com/${GITHUB_REPO}/${INSTALL_REF}/docker-compose.customer.yml"
elif [ -n "${VECTORAIZ_VERSION:-}" ]; then
    COMPOSE_URL="https://raw.githubusercontent.com/${GITHUB_REPO}/${VECTORAIZ_VERSION}/docker-compose.customer.yml"
else
    LATEST_TAG=$(curl -fsSL "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" 2>/dev/null | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')
    if [ -z "$LATEST_TAG" ]; then
        LATEST_TAG="main"
    fi
    COMPOSE_URL="https://raw.githubusercontent.com/${GITHUB_REPO}/${LATEST_TAG}/docker-compose.customer.yml"
fi
APP_BUNDLE="$HOME/Applications/vectorAIz.app"
PREFERRED_PORTS=(8080 3000 8888 9000 80)

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# --- Helpers ---
print_banner() {
    echo ""
    echo -e "${CYAN}${BOLD}"
    echo "  ╔═══════════════════════════════════════════╗"
    echo "  ║                                           ║"
    echo "  ║       ⚡ vectorAIz Installer ⚡           ║"
    echo "  ║                                           ║"
    echo "  ║   Self-hosted data processing & search    ║"
    echo "  ║                  macOS                    ║"
    echo "  ║                                           ║"
    echo "  ╚═══════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_ready() {
    echo ""
    echo -e "${GREEN}${BOLD}"
    echo "  ╔═══════════════════════════════════════════╗"
    echo "  ║                                           ║"
    echo "  ║       ✅ vectorAIz is Installed!          ║"
    echo -e "  ║          version ${VECTORAIZ_APP_VERSION}$(printf '%*s' $((24 - ${#VECTORAIZ_APP_VERSION})) '')║"
    echo "  ║                                           ║"
    echo "  ║   Open your browser to:                   ║"
    echo "  ║                                           ║"
    echo -e "  ║   ${BOLD}${CYAN}➜  ${URL} $(printf '%*s' $((25 - ${#URL})) '')${GREEN}║"
    echo "  ║                                           ║"
    echo "  ║   App: ~/Applications/vectorAIz.app       ║"
    echo "  ║   Data: ~/vectoraiz/                      ║"
    echo "  ║                                           ║"
    echo "  ║   To uninstall: run uninstall-mac.sh      ║"
    echo "  ║                                           ║"
    echo "  ╚═══════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_failed() {
    echo ""
    echo -e "${RED}${BOLD}"
    echo "  ╔═══════════════════════════════════════════╗"
    echo "  ║                                           ║"
    echo "  ║     ❌ vectorAIz failed to start          ║"
    echo "  ║                                           ║"
    echo "  ║   The health check timed out.             ║"
    echo "  ║                                           ║"
    echo "  ║   Troubleshooting:                        ║"
    echo "  ║                                           ║"
    echo "  ║   1. Check logs:                          ║"
    echo -e "  ║   ${NC}${DIM}cd ~/vectoraiz${RED}${BOLD}                           ║"
    echo -e "  ║   ${NC}${DIM}docker compose -f $COMPOSE_FILE logs${RED}${BOLD}     ║"
    echo "  ║                                           ║"
    echo "  ║   2. Retry:                               ║"
    echo -e "  ║   ${NC}${DIM}docker compose -f $COMPOSE_FILE restart${RED}${BOLD}  ║"
    echo "  ║                                           ║"
    echo "  ║   3. Reinstall:                           ║"
    echo -e "  ║   ${NC}${DIM}curl -fsSL get.vectoraiz.com | bash${RED}${BOLD}      ║"
    echo "  ║                                           ║"
    echo "  ╚═══════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_warning() {
    echo ""
    echo -e "${YELLOW}${BOLD}"
    echo "  ╔═══════════════════════════════════════════╗"
    echo "  ║                                           ║"
    echo "  ║    ⚠  vectorAIz may not be ready yet     ║"
    echo "  ║                                           ║"
    echo "  ║   Health check did not pass in time.      ║"
    echo "  ║   Services may still be loading.          ║"
    echo "  ║                                           ║"
    echo "  ║   Troubleshooting:                        ║"
    echo "  ║                                           ║"
    echo "  ║   1. Check logs:                          ║"
    echo -e "  ║   ${NC}${DIM}cd ~/vectoraiz${YELLOW}${BOLD}                           ║"
    echo -e "  ║   ${NC}${DIM}docker compose -f $COMPOSE_FILE logs${YELLOW}${BOLD}     ║"
    echo "  ║                                           ║"
    echo "  ║   2. Check health endpoint:               ║"
    echo -e "  ║   ${NC}${DIM}curl localhost:${PORT}/api/health${YELLOW}${BOLD}           ║"
    echo "  ║                                           ║"
    echo "  ║   3. Container status:                    ║"
    echo -e "  ║   ${NC}${DIM}docker compose -f $COMPOSE_FILE ps${YELLOW}${BOLD}       ║"
    echo "  ║                                           ║"
    echo "  ║   4. Restart:                             ║"
    echo -e "  ║   ${NC}${DIM}docker compose -f $COMPOSE_FILE restart${YELLOW}${BOLD}  ║"
    echo "  ║                                           ║"
    echo "  ╚═══════════════════════════════════════════╝"
    echo -e "${NC}"
}

fail() {
    echo -e "\n  ${RED}${BOLD}ERROR:${NC} $1\n"
    exit 1
}

info() {
    echo -e "  ${BLUE}▸${NC} $1"
}

success() {
    echo -e "  ${GREEN}✓${NC} $1"
}

warn() {
    echo -e "  ${YELLOW}⚠${NC} $1"
}

generate_secret() {
    openssl rand -hex 16
}

is_port_free() {
    local port=$1
    ! lsof -i :"$port" -sTCP:LISTEN &>/dev/null
}

make_url() {
    local port=$1
    if [ "$port" = "80" ]; then
        echo "http://localhost"
    else
        echo "http://localhost:${port}"
    fi
}

# =============================================================================
# Main
# =============================================================================
print_banner

# ─── Step 1: Check / Install Docker ─────────────────────────────
info "Checking for Docker..."

if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
    success "Docker is running"
elif [ -d "/Applications/Docker.app" ] || [ -d "/Applications/OrbStack.app" ]; then
    warn "Docker is installed but not running."
    info "Starting Docker..."
    if [ -d "/Applications/Docker.app" ]; then
        open -a Docker
    elif [ -d "/Applications/OrbStack.app" ]; then
        open -a OrbStack
    fi
    info "Waiting for Docker to start..."
    DOCKER_WAIT=0
    DOCKER_MAX=240
    while ! docker version --format '{{.Server.Version}}' &>/dev/null 2>&1; do
        if [ $DOCKER_WAIT -ge $DOCKER_MAX ]; then
            fail "Docker did not start within ${DOCKER_MAX}s. Please start Docker Desktop manually and re-run this installer."
        fi
        printf "\r  ${BLUE}⏳${NC} Waiting for Docker daemon... (%ds)" "$DOCKER_WAIT"
        sleep 3
        DOCKER_WAIT=$((DOCKER_WAIT + 3))
    done
    printf "\r                                                          \r"
    success "Docker is running"
else
    warn "Docker is not installed."
    echo ""

    # Try Homebrew first
    if command -v brew &>/dev/null; then
        echo -e "  ${CYAN}Installing Docker Desktop via Homebrew...${NC}"
        echo ""
        brew install --cask docker
        echo ""
        info "Starting Docker Desktop..."
        open -a Docker

        info "Waiting for Docker to start (this may take a minute on first launch)..."
        DOCKER_WAIT=0
        DOCKER_MAX=300
        while ! docker version --format '{{.Server.Version}}' &>/dev/null 2>&1; do
            if [ $DOCKER_WAIT -ge $DOCKER_MAX ]; then
                fail "Docker did not start within ${DOCKER_MAX}s.\n  Please open Docker Desktop from your Applications folder and re-run this installer."
            fi
            printf "\r  ${BLUE}⏳${NC} Waiting for Docker daemon... (%ds)" "$DOCKER_WAIT"
            sleep 3
            DOCKER_WAIT=$((DOCKER_WAIT + 3))
        done
        printf "\r                                                          \r"
        success "Docker is installed and running"
    else
        echo -e "  Docker Desktop is required to run vectorAIz."
        echo ""
        echo -e "  ${BOLD}Option 1:${NC} Install Homebrew first, then re-run this installer:"
        echo -e "  ${DIM}/bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\"${NC}"
        echo ""
        echo -e "  ${BOLD}Option 2:${NC} Download Docker Desktop manually:"
        echo -e "  ${CYAN}https://docs.docker.com/desktop/install/mac-install/${NC}"
        echo ""
        echo -e "  After installing Docker, re-run this installer."
        exit 1
    fi
fi

# ─── Step 1.5: Clean up previous installation ───────────────────
EXISTING_CONTAINERS=$(docker ps -a --filter "name=vectoraiz-" --format "{{.Names}}" 2>/dev/null)
EXISTING_VOLUMES=$(docker volume ls --filter "name=vectoraiz_" --format "{{.Name}}" 2>/dev/null)

if [ -n "$EXISTING_CONTAINERS" ] || [ -n "$EXISTING_VOLUMES" ]; then
    warn "Existing vectorAIz installation detected (Docker containers/volumes)."
    info "Cleaning up previous installation..."

    if [ -f "$INSTALL_DIR/$COMPOSE_FILE" ]; then
        # Preferred: use compose to tear down cleanly (removes containers + volumes)
        cd "$INSTALL_DIR" && docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null
        cd "$HOME" 2>/dev/null || cd /tmp
    else
        # Fallback: manually stop/remove containers and volumes
        if [ -n "$EXISTING_CONTAINERS" ]; then
            echo "$EXISTING_CONTAINERS" | xargs -r docker stop 2>/dev/null
            echo "$EXISTING_CONTAINERS" | xargs -r docker rm 2>/dev/null
        fi
        if [ -n "$EXISTING_VOLUMES" ]; then
            echo "$EXISTING_VOLUMES" | xargs -r docker volume rm 2>/dev/null
        fi
    fi

    success "Previous installation cleaned up"
fi

# ─── Step 2: Create install directory ────────────────────────────
info "Setting up install directory..."

mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"
success "Install directory: $INSTALL_DIR"

# ─── Step 3: Download compose file ──────────────────────────────
info "Downloading docker-compose configuration..."

if curl -fsSL "$COMPOSE_URL" -o "$INSTALL_DIR/$COMPOSE_FILE"; then
    success "Downloaded $COMPOSE_FILE"
else
    fail "Failed to download compose file from GitHub.\n  Check your internet connection and try again."
fi

# Parse version: prefer VECTORAIZ_VERSION env var (set by RC wrapper), else fall back to compose default
if [ -n "${VECTORAIZ_VERSION:-}" ]; then
    VECTORAIZ_APP_VERSION="$VECTORAIZ_VERSION"
else
    VECTORAIZ_APP_VERSION=$(sed -n 's/.*VECTORAIZ_VERSION:-\([^}]*\)}.*/\1/p' "$INSTALL_DIR/$COMPOSE_FILE" | head -1)
    VECTORAIZ_APP_VERSION="${VECTORAIZ_APP_VERSION:-latest}"
fi
info "vectorAIz version: ${BOLD}${VECTORAIZ_APP_VERSION}${NC}"

# ─── Step 4: Find available port ─────────────────────────────────
info "Finding available port..."

PORT=""
if [ -f "$INSTALL_DIR/.env" ] && grep -q "^VECTORAIZ_PORT=" "$INSTALL_DIR/.env" 2>/dev/null; then
    PORT=$(grep "^VECTORAIZ_PORT=" "$INSTALL_DIR/.env" | cut -d'=' -f2 | tr -d ' "'"'"'')
    if ! is_port_free "$PORT"; then
        warn "Previously configured port $PORT is in use. Finding another..."
        PORT=""
    fi
fi

if [ -z "$PORT" ]; then
    for TRY_PORT in "${PREFERRED_PORTS[@]}"; do
        if is_port_free "$TRY_PORT"; then
            PORT="$TRY_PORT"
            break
        fi
    done
fi

if [ -z "$PORT" ]; then
    PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()" 2>/dev/null || echo "8080")
fi

success "Using port $PORT"
URL=$(make_url "$PORT")


# ─── Step 5a: Connected mode prompt ──────────────────────────────
VECTORAIZ_MODE="standalone"
if [ ! -f "$INSTALL_DIR/.env" ]; then
    if [ ! -t 0 ] && [ ! -e /dev/tty ]; then
        # Non-interactive mode (e.g. CI / piped input without TTY)
        if [ -n "${VECTORAIZ_MODE:-}" ]; then
            success "Using VECTORAIZ_MODE=${VECTORAIZ_MODE} from environment"
        else
            VECTORAIZ_MODE="standalone"
            info "Non-interactive install detected, defaulting to standalone mode. Set VECTORAIZ_MODE=connected to enable ai.market features."
        fi
    else
        echo ""
        echo "  ┌─────────────────────────────────────────────────────────┐"
        echo "  │  Would you like to run vectorAIz in Connected mode?    │"
        echo "  │                                                         │"
        echo "  │  YES — Enables allAI, your AI data assistant            │"
        echo "  │  NO  — Standalone mode, no internet access required     │"
        echo "  └─────────────────────────────────────────────────────────┘"
        echo ""
        while true; do
            printf "  Connect to ai.market for AI features? (Y/N): "
            read -r yn </dev/tty
            case "$yn" in
                [Yy]* ) VECTORAIZ_MODE="connected"; success "Connected mode selected — allAI will be available"; break;;
                [Nn]* ) VECTORAIZ_MODE="standalone"; success "Standalone mode selected"; break;;
                * ) echo "  Please answer Y or N.";;
            esac
        done
    fi
fi

# ─── Step 5: Generate .env ───────────────────────────────────────
# On reinstall the .env won't exist because the user removed ~/vectoraiz.
# The matching Docker volumes were already cleaned up in Step 1.5, so a fresh
# POSTGRES_PASSWORD here will match the fresh postgres volume created at start.
if [ ! -f "$INSTALL_DIR/.env" ]; then
    info "Generating secure configuration..."
    cat > "$INSTALL_DIR/.env" <<EOF
# vectorAIz Configuration
# Generated on $(date -u +"%Y-%m-%d %H:%M:%S UTC")
# Install directory: $INSTALL_DIR

# Database password (auto-generated, keep this safe)
POSTGRES_PASSWORD=$(generate_secret)

# Application secrets
VECTORAIZ_SECRET_KEY=$(generate_secret)
VECTORAIZ_APIKEY_HMAC_SECRET=$(generate_secret)

# Port to serve on
VECTORAIZ_PORT=${PORT}

# Mode: standalone or connected (with allAI)
VECTORAIZ_MODE=${VECTORAIZ_MODE}

# Local import directory (mounted read-only for direct file access)
VECTORAIZ_IMPORT_DIR=${HOME}/vectoraiz-imports
EOF
    success "Generated .env with secure defaults"
else
    # Update port in existing .env
    if grep -q "^VECTORAIZ_PORT=" "$INSTALL_DIR/.env" 2>/dev/null; then
        sed -i.bak "s/^VECTORAIZ_PORT=.*/VECTORAIZ_PORT=${PORT}/" "$INSTALL_DIR/.env" && rm -f "$INSTALL_DIR/.env.bak"
    else
        echo "VECTORAIZ_PORT=${PORT}" >> "$INSTALL_DIR/.env"
    fi
    success "Using existing .env (port updated to ${PORT})"
fi

# Persist version override to .env (ensures RC versions survive restart)
if [ -n "${VECTORAIZ_VERSION:-}" ]; then
    if grep -q "^VECTORAIZ_VERSION=" "$INSTALL_DIR/.env" 2>/dev/null; then
        sed -i.bak "s/^VECTORAIZ_VERSION=.*/VECTORAIZ_VERSION=${VECTORAIZ_VERSION}/" "$INSTALL_DIR/.env" && rm -f "$INSTALL_DIR/.env.bak"
    else
        echo "" >> "$INSTALL_DIR/.env"
        echo "# Docker image version (set by installer)" >> "$INSTALL_DIR/.env"
        echo "VECTORAIZ_VERSION=${VECTORAIZ_VERSION}" >> "$INSTALL_DIR/.env"
    fi
fi

# Create local import directory (mounted read-only into the container)
mkdir -p "${HOME}/vectoraiz-imports"

# ─── Step 5b: Provision serial for connected mode ─────────────────
if grep -q "^VECTORAIZ_MODE=connected" "$INSTALL_DIR/.env" 2>/dev/null; then
    info "Provisioning serial for allAI..."

    # Determine Docker volume name (compose project = directory basename)
    COMPOSE_PROJECT=$(basename "$INSTALL_DIR")
    SERIAL_VOLUME="${COMPOSE_PROJECT}_vectoraiz-data"
    docker volume create "$SERIAL_VOLUME" >/dev/null 2>&1 || true

    # Check if serial.json already exists (upgrade path — do NOT overwrite)
    EXISTING_SERIAL=$(docker run --rm -v "${SERIAL_VOLUME}:/data" alpine cat /data/serial.json 2>/dev/null || echo "")

    if [ -n "$EXISTING_SERIAL" ]; then
        success "Existing serial found — preserving (upgrade path)"
    else
        SERIAL_RESPONSE=$(curl -s --max-time 10 -X POST "https://api.ai.market/api/v1/serials/generate" \
            -H "Content-Type: application/json" -d '{}' 2>/dev/null || echo "")

        if [ -n "$SERIAL_RESPONSE" ]; then
            SERIAL_VAL=$(echo "$SERIAL_RESPONSE" | grep -o '"serial" *: *"[^"]*"' | head -1 | sed 's/.*: *"\([^"]*\)"/\1/')
            BOOTSTRAP_VAL=$(echo "$SERIAL_RESPONSE" | grep -o '"bootstrap_token" *: *"[^"]*"' | head -1 | sed 's/.*: *"\([^"]*\)"/\1/')

            if [ -n "$SERIAL_VAL" ] && [ -n "$BOOTSTRAP_VAL" ]; then
                printf '{"serial": "%s", "bootstrap_token": "%s", "state": "provisioned"}' "$SERIAL_VAL" "$BOOTSTRAP_VAL" | \
                    docker run --rm -i -v "${SERIAL_VOLUME}:/data" alpine sh -c 'cat > /data/serial.json && chmod 600 /data/serial.json'
                success "Serial provisioned: ${SERIAL_VAL}"
            else
                warn "Failed to parse serial response — allAI may require reinstall"
            fi
        else
            warn "Could not reach serial API — allAI may require reinstall"
        fi
    fi
fi

# ─── Step 6: Pull images ─────────────────────────────────────────
info "Pulling Docker images (this may take a few minutes)..."
echo ""

cd "$INSTALL_DIR"
docker compose -f "$COMPOSE_FILE" pull 2>&1 | while IFS= read -r line; do
    case "$line" in
        *"Pulling"*|*"Downloaded"*|*"Pull"*|*"Image"*|*"Up to date"*|*"digest"*)
            echo -e "  ${CYAN}│${NC} $line"
            ;;
    esac
done
echo ""
success "All images pulled"

# ─── Step 7: Start containers ────────────────────────────────────
info "Starting vectorAIz..."

docker compose -f "$COMPOSE_FILE" up -d 2>&1 | while IFS= read -r line; do
    case "$line" in
        *"Created"*|*"Started"*|*"Running"*)
            echo -e "  ${CYAN}│${NC} $line"
            ;;
    esac
done

# ─── Step 8: Wait for health check ───────────────────────────────
info "Waiting for vectorAIz to be ready..."
MAX_WAIT=120
WAITED=0
HEALTH_OK=false
while [ $WAITED -lt $MAX_WAIT ]; do
    if curl -sf "http://localhost:${PORT}/api/health" >/dev/null 2>&1; then
        HEALTH_OK=true
        break
    fi
    printf "\r  ${BLUE}⏳${NC} Waiting for services to initialize... (%ds)" "$WAITED"
    sleep 3
    WAITED=$((WAITED + 3))
done
printf "\r                                                          \r"

if [ "$HEALTH_OK" = true ]; then
    success "All services healthy"
else
    warn "Timed out waiting for health check after ${MAX_WAIT}s."
fi

# ─── Step 8.5: Verify all containers are running ────────────────
info "Verifying container status..."
CONTAINERS_OK=true
for SVC in postgres qdrant vectoraiz; do
    SVC_STATE=$(docker compose -f "$COMPOSE_FILE" ps "$SVC" --format '{{.State}}' 2>/dev/null)
    if echo "$SVC_STATE" | grep -qi "running"; then
        success "$SVC is running"
    else
        warn "$SVC is not running (state: ${SVC_STATE:-not found})"
        CONTAINERS_OK=false
    fi
done

# ─── Step 9: Create .app bundle ──────────────────────────────────
info "Creating vectorAIz.app..."

mkdir -p "$HOME/Applications"
rm -rf "$APP_BUNDLE"
mkdir -p "$APP_BUNDLE/Contents/MacOS"
mkdir -p "$APP_BUNDLE/Contents/Resources"

# Create the launcher script
cat > "$APP_BUNDLE/Contents/MacOS/vectorAIz" <<'LAUNCHER'
#!/bin/bash
INSTALL_DIR="$HOME/vectoraiz"
COMPOSE_FILE="docker-compose.customer.yml"

cd "$INSTALL_DIR" || exit 1

# Read port from .env
PORT=$(grep "^VECTORAIZ_PORT=" .env 2>/dev/null | cut -d'=' -f2 | tr -d ' "'"'"'')
PORT="${PORT:-80}"

if [ "$PORT" = "80" ]; then
    URL="http://localhost"
else
    URL="http://localhost:${PORT}"
fi

# Start Docker if not running
if ! docker info &>/dev/null 2>&1; then
    if [ -d "/Applications/OrbStack.app" ]; then
        open -a OrbStack
    elif [ -d "/Applications/Docker.app" ]; then
        open -a Docker
    fi
    # Wait up to 60s for Docker
    for i in $(seq 1 20); do
        docker info &>/dev/null 2>&1 && break
        sleep 3
    done
fi

# Start containers
docker compose -f "$COMPOSE_FILE" up -d 2>/dev/null

# Wait for health then open browser
for i in $(seq 1 40); do
    if curl -sf "${URL}/api/health" >/dev/null 2>&1; then
        open "$URL"
        exit 0
    fi
    sleep 3
done

# Open anyway after timeout
open "$URL"
LAUNCHER
chmod +x "$APP_BUNDLE/Contents/MacOS/vectorAIz"

# Create Info.plist
cat > "$APP_BUNDLE/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>vectorAIz</string>
    <key>CFBundleIdentifier</key>
    <string>com.vectoraiz.app</string>
    <key>CFBundleName</key>
    <string>vectorAIz</string>
    <key>CFBundleDisplayName</key>
    <string>vectorAIz</string>
    <key>CFBundleVersion</key>
    <string>${VECTORAIZ_APP_VERSION}</string>
    <key>CFBundleShortVersionString</key>
    <string>${VECTORAIZ_APP_VERSION}</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>LSMinimumSystemVersion</key>
    <string>10.15</string>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
PLIST

success "Created ~/Applications/vectorAIz.app"

# ─── Step 10: Open browser ───────────────────────────────────────
if [ "$HEALTH_OK" = true ] && [ "$CONTAINERS_OK" = true ]; then
    print_ready

    sleep 1
    open "$URL" 2>/dev/null || true

    echo -e "  ${CYAN}Tip:${NC} Launch vectorAIz anytime from ~/Applications/vectorAIz.app"
    echo -e "  ${CYAN}Tip:${NC} Add files to ~/vectoraiz-imports/ for fast local import (no upload needed)"
    echo -e "  ${CYAN}Tip:${NC} View logs: cd ~/vectoraiz && docker compose -f $COMPOSE_FILE logs -f"
    echo -e "  ${CYAN}Tip:${NC} Stop: cd ~/vectoraiz && docker compose -f $COMPOSE_FILE down"
    echo ""
else
    echo ""
    echo -e "  ${YELLOW}${BOLD}Recent vectoraiz container logs:${NC}"
    echo -e "  ${DIM}────────────────────────────────────────────${NC}"
    docker compose -f "$COMPOSE_FILE" logs --tail=20 vectoraiz 2>/dev/null | while IFS= read -r line; do
        echo -e "  ${DIM}│${NC} $line"
    done
    echo -e "  ${DIM}────────────────────────────────────────────${NC}"

    print_warning
fi
