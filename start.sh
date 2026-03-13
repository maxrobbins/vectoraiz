#!/bin/bash
# =============================================================================
# vectorAIz — One-Click Setup
# =============================================================================
# Usage: ./start.sh
#
# What this does:
#   1. Checks Docker is installed and running
#   2. Checks for port conflicts and finds a free port
#   3. Generates secrets if first run
#   4. Pulls and starts all containers
#   5. Waits for the app to be healthy
#   6. Creates a desktop shortcut
#   7. Opens your browser
# =============================================================================

set -e

# --- Configuration ---
COMPOSE_FILE="docker-compose.customer.yml"
APP_NAME="vectorAIz"
SHORTCUT_NAME="vectorAIz"
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
    echo "  ║           ⚡ vectorAIz Setup ⚡           ║"
    echo "  ║                                           ║"
    echo "  ║   Self-hosted data processing & search    ║"
    echo "  ║                                           ║"
    echo "  ╚═══════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_ready() {
    echo ""
    echo -e "${GREEN}${BOLD}"
    echo "  ╔═══════════════════════════════════════════╗"
    echo "  ║                                           ║"
    echo "  ║          ✅ vectorAIz is Ready!           ║"
    echo "  ║                                           ║"
    echo "  ║   Open your browser to:                   ║"
    echo "  ║                                           ║"
    echo -e "  ║   ${BOLD}${CYAN}➜  ${URL} $(printf '%*s' $((25 - ${#URL})) '')${GREEN}║"
    echo "  ║                                           ║"
    echo "  ║   To stop: ./stop.sh                      ║"
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

is_port_free() {
    local port=$1
    if command -v lsof &>/dev/null; then
        ! lsof -i :"$port" -sTCP:LISTEN &>/dev/null
    elif command -v ss &>/dev/null; then
        ! ss -tlnp | grep -q ":${port} "
    elif command -v netstat &>/dev/null; then
        ! netstat -tlnp 2>/dev/null | grep -q ":${port} "
    else
        ! (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null
    fi
}

get_port_process() {
    local port=$1
    if command -v lsof &>/dev/null; then
        lsof -i :"$port" -sTCP:LISTEN -t 2>/dev/null | head -1 | xargs -I{} ps -p {} -o comm= 2>/dev/null
    fi
}

make_url() {
    local port=$1
    if [ "$port" = "80" ]; then
        echo "http://localhost"
    else
        echo "http://localhost:${port}"
    fi
}

# --- Main ---
print_banner

# ─── Step 1: Check Docker ───────────────────────────────────────
info "Checking Docker..."
if ! command -v docker &>/dev/null; then
    fail "Docker is not installed.\n\n  Install Docker Desktop: https://docker.com/get-started\n  Or OrbStack (recommended for Mac): https://orbstack.dev"
fi

if ! docker info &>/dev/null 2>&1; then
    fail "Docker is not running. Please start Docker Desktop or OrbStack first."
fi
success "Docker is running"

# ─── Step 2: Find compose file ──────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f "$COMPOSE_FILE" ]; then
    fail "Cannot find $COMPOSE_FILE in $(pwd)"
fi

# ─── Step 2b: Clean stale Docker resources from previous install ─
EXISTING_VOLUMES=$(docker volume ls --filter "name=vectoraiz_" --format "{{.Name}}" 2>/dev/null)
if [ -n "$EXISTING_VOLUMES" ]; then
    info "Found existing vectorAIz volumes. Cleaning up stale data..."
    # Stop any running containers first
    if [ -f "$COMPOSE_FILE" ]; then
        docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    else
        # No compose file, remove volumes directly
        docker container ls -a --filter "name=vectoraiz" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
        echo "$EXISTING_VOLUMES" | xargs -r docker volume rm 2>/dev/null || true
    fi
    success "Cleaned up previous installation"
fi

EXISTING_CONTAINERS=$(docker container ls -a --filter "name=vectoraiz" --format "{{.ID}}" 2>/dev/null)
if [ -n "$EXISTING_CONTAINERS" ]; then
    info "Removing stale containers..."
    echo "$EXISTING_CONTAINERS" | xargs -r docker rm -f 2>/dev/null || true
fi

# ─── Step 3: Port detection ─────────────────────────────────────
info "Checking for available port..."

if [ -n "$VECTORAIZ_PORT" ]; then
    PORT="$VECTORAIZ_PORT"
elif [ -f ".env" ] && grep -q "^VECTORAIZ_PORT=" .env 2>/dev/null; then
    PORT=$(grep "^VECTORAIZ_PORT=" .env | cut -d'=' -f2 | tr -d ' "'"'"'')
fi

if [ -n "$PORT" ]; then
    if is_port_free "$PORT"; then
        success "Port $PORT is available"
    else
        OCCUPANT=$(get_port_process "$PORT")
        warn "Port $PORT is in use${OCCUPANT:+ by $OCCUPANT}"
        echo ""
        echo -e "    ${BOLD}1)${NC} Pick a free port automatically"
        echo -e "    ${BOLD}2)${NC} Enter a specific port"
        echo -e "    ${BOLD}3)${NC} Abort"
        echo ""
        read -rp "  Choice [1/2/3]: " CHOICE < /dev/tty
        case "$CHOICE" in
            1) PORT="" ;;
            2)
                read -rp "  Enter port number: " PORT < /dev/tty
                if ! is_port_free "$PORT"; then
                    fail "Port $PORT is also in use."
                fi
                success "Port $PORT is available"
                ;;
            *) echo ""; info "Run again after freeing the port."; exit 0 ;;
        esac
    fi
fi

if [ -z "$PORT" ]; then
    for TRY_PORT in "${PREFERRED_PORTS[@]}"; do
        if is_port_free "$TRY_PORT"; then
            PORT="$TRY_PORT"
            break
        fi
    done
    if [ -z "$PORT" ]; then
        PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()" 2>/dev/null || echo "8080")
    fi
    if [ "$PORT" != "80" ]; then
        info "Port 80 is in use — using port ${PORT} instead"
    fi
    success "Using port $PORT"
fi

URL=$(make_url "$PORT")
export VECTORAIZ_PORT="$PORT"

# ─── Step 4: Generate .env ──────────────────────────────────────
FIRST_RUN=false

if [ ! -f ".env" ]; then
    FIRST_RUN=true
    info "First run detected — generating configuration..."
    POSTGRES_PW=$(openssl rand -hex 16)

    # Check for API key (backward compat — if provided via env, use it)
    API_KEY="${VECTORAIZ_API_KEY:-${VECTORAIZ_INTERNAL_API_KEY:-}}"

    echo ""
    echo -e "  ${CYAN}${BOLD}allAI Setup${NC}"
    echo -e "  allAI will be configured automatically on first launch."
    echo ""

    cat > .env <<EOF
# vectorAIz Configuration
# Generated on $(date -u +"%Y-%m-%d %H:%M:%S UTC")

# Database password (auto-generated, keep this safe)
POSTGRES_PASSWORD=${POSTGRES_PW}

# Port to serve on
VECTORAIZ_PORT=${PORT}

# allAI enabled (connected mode)
VECTORAIZ_MODE=connected
VECTORAIZ_AI_MARKET_URL=https://ai-market-backend-production.up.railway.app
VECTORAIZ_ALLIE_PROVIDER=aimarket
EOF

    if [ -n "$API_KEY" ]; then
        echo "VECTORAIZ_INTERNAL_API_KEY=${API_KEY}" >> .env
        success "Generated .env with allAI enabled (API key provided)"
    else
        success "Generated .env with allAI enabled (serial auto-provisioning)"
    fi
else
    if grep -q "^VECTORAIZ_PORT=" .env 2>/dev/null; then
        sed -i.bak "s/^VECTORAIZ_PORT=.*/VECTORAIZ_PORT=${PORT}/" .env && rm -f .env.bak
    else
        echo "VECTORAIZ_PORT=${PORT}" >> .env
    fi
    success "Using existing .env (port: ${PORT})"
fi

# ─── Step 5: Pull and start ─────────────────────────────────────
info "Starting vectorAIz..."
echo ""

docker compose -f "$COMPOSE_FILE" pull 2>&1 | while IFS= read -r line; do
    case "$line" in
        *"Pulling"*|*"Downloaded"*|*"Pull"*|*"Image"*|*"Up to date"*|*"digest"*)
            echo -e "  ${CYAN}│${NC} $line"
            ;;
    esac
done

docker compose -f "$COMPOSE_FILE" up -d 2>&1 | while IFS= read -r line; do
    case "$line" in
        *"Created"*|*"Started"*|*"Running"*)
            echo -e "  ${CYAN}│${NC} $line"
            ;;
    esac
done

echo ""

# ─── Step 6: Wait for healthy ───────────────────────────────────
info "Waiting for vectorAIz to be ready..."
MAX_WAIT=120
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    if curl -sf "http://localhost:${PORT}/api/health" >/dev/null 2>&1; then
        break
    fi

    CONTAINER_STATUS=$(docker compose -f "$COMPOSE_FILE" ps --format json 2>/dev/null | grep vectoraiz | grep -o '"State":"[^"]*"' | cut -d'"' -f4)
    if [ "$CONTAINER_STATUS" = "restarting" ] && [ $WAITED -gt 30 ]; then
        echo ""
        warn "vectorAIz container is restarting. Checking logs..."
        echo ""
        docker compose -f "$COMPOSE_FILE" logs vectoraiz 2>&1 | tail -10 | while IFS= read -r line; do
            echo -e "  ${RED}│${NC} $line"
        done
        echo ""
        fail "Container failed to start. Check full logs with: docker compose -f $COMPOSE_FILE logs vectoraiz"
    fi

    printf "\r  ${BLUE}⏳${NC} Waiting for services to initialize... (%ds)" "$WAITED"
    sleep 3
    WAITED=$((WAITED + 3))
done
printf "\r                                                          \r"

if [ $WAITED -ge $MAX_WAIT ]; then
    warn "Timed out waiting for health check."
    echo -e "  Check logs: ${BOLD}docker compose -f $COMPOSE_FILE logs${NC}"
    echo -e "  The app may still be starting. Try opening ${BOLD}$URL${NC} in a minute."
    echo -e "  ${YELLOW}If this is a reinstall, try:${NC} docker volume rm vectoraiz_postgres-data && rerun the installer"
else
    success "All services healthy"
fi

# ─── Step 7: Desktop shortcut ───────────────────────────────────
create_webloc() {
    local dir="$1"
    local shortcut_path="${dir}/${SHORTCUT_NAME}.webloc"
    if [ -d "$dir" ]; then
        cat > "$shortcut_path" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>URL</key>
    <string>${URL}</string>
</dict>
</plist>
EOF
        return 0
    fi
    return 1
}

create_desktop_file() {
    local filepath="$1"
    cat > "$filepath" <<EOF
[Desktop Entry]
Type=Link
Name=vectorAIz
URL=${URL}
Icon=applications-internet
EOF
    chmod +x "$filepath" 2>/dev/null
}

if [[ "$OSTYPE" == "darwin"* ]]; then
    if create_webloc "$HOME/Desktop"; then
        success "Desktop shortcut created: ~/Desktop/${SHORTCUT_NAME}.webloc"
    fi
    if create_webloc "$SCRIPT_DIR"; then
        success "Local shortcut created: ./${SHORTCUT_NAME}.webloc"
    fi
elif [[ "$OSTYPE" == "linux"* ]]; then
    if [ -d "$HOME/Desktop" ]; then
        create_desktop_file "${HOME}/Desktop/${SHORTCUT_NAME}.desktop"
        success "Desktop shortcut created"
    fi
    create_desktop_file "${SCRIPT_DIR}/${SHORTCUT_NAME}.desktop"
fi

# ─── Step 8: Done ───────────────────────────────────────────────
print_ready

if [[ "$OSTYPE" == "darwin"* ]]; then
    sleep 1
    open "$URL" 2>/dev/null || true
elif command -v xdg-open &>/dev/null; then
    sleep 1
    xdg-open "$URL" 2>/dev/null || true
fi

echo -e "  ${CYAN}Tip:${NC} View logs with: docker compose -f $COMPOSE_FILE logs -f"
echo ""
