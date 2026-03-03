#!/bin/bash
# =============================================================================
# vectorAIz — Linux Installer
# =============================================================================
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/aidotmarket/vectoraiz/main/installers/linux/install-linux.sh | bash
#
# Or download and run:
#   chmod +x install-linux.sh && ./install-linux.sh
# =============================================================================

set -e

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
    echo "  ║                  Linux                    ║"
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
    echo "  ║                                           ║"
    echo "  ║   Open your browser to:                   ║"
    echo "  ║                                           ║"
    echo -e "  ║   ${BOLD}${CYAN}➜  ${URL} $(printf '%*s' $((25 - ${#URL})) '')${GREEN}║"
    echo "  ║                                           ║"
    echo "  ║   Data: ~/vectoraiz/                      ║"
    echo "  ║                                           ║"
    echo "  ║   To uninstall: run uninstall-linux.sh    ║"
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
    openssl rand -hex 16 2>/dev/null || head -c 16 /dev/urandom | xxd -p 2>/dev/null || python3 -c "import secrets; print(secrets.token_hex(16))"
}

is_port_free() {
    local port=$1
    if command -v ss &>/dev/null; then
        ! ss -tlnp 2>/dev/null | grep -q ":${port} "
    elif command -v netstat &>/dev/null; then
        ! netstat -tlnp 2>/dev/null | grep -q ":${port} "
    else
        ! (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null
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

detect_distro() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        case "$ID" in
            ubuntu|debian|pop|linuxmint|elementary|zorin)
                echo "debian"
                ;;
            fedora|rhel|centos|rocky|alma)
                echo "fedora"
                ;;
            arch|manjaro|endeavouros)
                echo "arch"
                ;;
            opensuse*|sles)
                echo "suse"
                ;;
            *)
                echo "unknown"
                ;;
        esac
    else
        echo "unknown"
    fi
}

# =============================================================================
# Main
# =============================================================================
print_banner

# ─── Step 1: Detect distro ──────────────────────────────────────
DISTRO=$(detect_distro)
if [ -f /etc/os-release ]; then
    . /etc/os-release
    info "Detected: ${PRETTY_NAME:-$ID}"
else
    info "Detected: Linux (unknown distribution)"
fi

# ─── Step 2: Check / Install Docker ─────────────────────────────
info "Checking for Docker..."

DOCKER_RUNNING=false
if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
    DOCKER_RUNNING=true
    success "Docker is running"
elif command -v docker &>/dev/null; then
    warn "Docker is installed but not running."

    # Try to start Docker service
    if command -v systemctl &>/dev/null; then
        info "Starting Docker service..."
        sudo systemctl start docker 2>/dev/null || true
        sleep 2
        if docker info &>/dev/null 2>&1; then
            DOCKER_RUNNING=true
            success "Docker is running"
        fi
    fi

    if [ "$DOCKER_RUNNING" = false ]; then
        # Check if user is in docker group
        if ! groups | grep -q docker; then
            warn "Your user is not in the 'docker' group."
            info "Adding you to the docker group..."
            sudo usermod -aG docker "$USER"
            echo ""
            echo -e "  ${YELLOW}${BOLD}You need to log out and log back in for group changes to take effect.${NC}"
            echo -e "  ${DIM}Then re-run this installer.${NC}"
            echo ""
            echo -e "  ${DIM}Or run: newgrp docker && bash install-linux.sh${NC}"
            exit 1
        fi
        fail "Docker is not running. Start it with: sudo systemctl start docker"
    fi
else
    warn "Docker is not installed."
    echo ""

    case "$DISTRO" in
        debian)
            echo -e "  ${CYAN}Installing Docker via apt...${NC}"
            echo ""
            sudo apt-get update -qq
            sudo apt-get install -y -qq docker.io docker-compose-plugin
            ;;
        fedora)
            echo -e "  ${CYAN}Installing Docker via dnf...${NC}"
            echo ""
            sudo dnf install -y docker docker-compose-plugin
            ;;
        arch)
            echo -e "  ${CYAN}Installing Docker via pacman...${NC}"
            echo ""
            sudo pacman -S --noconfirm docker docker-compose
            ;;
        *)
            echo -e "  Please install Docker manually:"
            echo -e "  ${CYAN}https://docs.docker.com/engine/install/${NC}"
            echo ""
            echo -e "  After installing Docker, re-run this installer."
            exit 1
            ;;
    esac

    # Add user to docker group
    if ! groups | grep -q docker; then
        sudo usermod -aG docker "$USER"
    fi

    # Enable and start Docker
    if command -v systemctl &>/dev/null; then
        sudo systemctl enable docker
        sudo systemctl start docker
    fi

    # Wait for Docker
    sleep 3
    if docker info &>/dev/null 2>&1; then
        DOCKER_RUNNING=true
        success "Docker is installed and running"
    elif sudo docker info &>/dev/null 2>&1; then
        # Docker works with sudo but not without — group not active yet
        echo ""
        echo -e "  ${YELLOW}${BOLD}Docker is installed but requires a logout/login for group permissions.${NC}"
        echo ""
        echo -e "  ${DIM}Option 1: Log out and log back in, then re-run this installer.${NC}"
        echo -e "  ${DIM}Option 2: Run: newgrp docker && bash install-linux.sh${NC}"
        exit 1
    else
        fail "Docker installation failed. Please install Docker manually:\n  https://docs.docker.com/engine/install/"
    fi
fi

# ─── Step 3: Check docker compose ───────────────────────────────
if docker compose version &>/dev/null 2>&1; then
    success "Docker Compose available"
else
    fail "Docker Compose plugin not found.\n  Install it with your package manager or see:\n  https://docs.docker.com/compose/install/linux/"
fi

# ─── Step 4: Create install directory ────────────────────────────
info "Setting up install directory..."
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"
success "Install directory: $INSTALL_DIR"

# ─── Step 5: Download compose file ──────────────────────────────
info "Downloading docker-compose configuration..."
if curl -fsSL "$COMPOSE_URL" -o "$INSTALL_DIR/$COMPOSE_FILE"; then
    success "Downloaded $COMPOSE_FILE"
else
    fail "Failed to download compose file from GitHub.\n  Check your internet connection and try again."
fi

# ─── Step 6: Find available port ─────────────────────────────────
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


# ─── Step 6a: Connected mode prompt ──────────────────────────────
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

# ─── Step 7: Generate .env ───────────────────────────────────────
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
EOF
    success "Generated .env with secure defaults"
else
    if grep -q "^VECTORAIZ_PORT=" "$INSTALL_DIR/.env" 2>/dev/null; then
        sed -i "s/^VECTORAIZ_PORT=.*/VECTORAIZ_PORT=${PORT}/" "$INSTALL_DIR/.env"
    else
        echo "VECTORAIZ_PORT=${PORT}" >> "$INSTALL_DIR/.env"
    fi
    success "Using existing .env (port updated to ${PORT})"
fi

# ─── Step 8: Pull images ─────────────────────────────────────────
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

# ─── Step 9: Start containers ────────────────────────────────────
info "Starting vectorAIz..."

docker compose -f "$COMPOSE_FILE" up -d 2>&1 | while IFS= read -r line; do
    case "$line" in
        *"Created"*|*"Started"*|*"Running"*)
            echo -e "  ${CYAN}│${NC} $line"
            ;;
    esac
done

# ─── Step 10: Wait for health check ──────────────────────────────
info "Waiting for vectorAIz to be ready..."
MAX_WAIT=300
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

# ─── Step 11: Create .desktop file ───────────────────────────────
info "Creating desktop entry..."

DESKTOP_DIR="$HOME/.local/share/applications"
mkdir -p "$DESKTOP_DIR"

cat > "$DESKTOP_DIR/vectoraiz.desktop" <<EOF
[Desktop Entry]
Name=vectorAIz
Comment=Self-hosted data processing & search
Exec=bash -c 'cd $INSTALL_DIR && docker compose -f $COMPOSE_FILE up -d && sleep 5 && xdg-open $URL'
Icon=applications-science
Type=Application
Categories=Development;Database;Science;
Terminal=false
StartupNotify=true
EOF

# Validate desktop file if tool is available
if command -v desktop-file-validate &>/dev/null; then
    desktop-file-validate "$DESKTOP_DIR/vectoraiz.desktop" 2>/dev/null || true
fi

# Update desktop database if available
if command -v update-desktop-database &>/dev/null; then
    update-desktop-database "$DESKTOP_DIR" 2>/dev/null || true
fi

success "Desktop entry created"

# Also create shortcut on Desktop if it exists
if [ -d "$HOME/Desktop" ]; then
    cp "$DESKTOP_DIR/vectoraiz.desktop" "$HOME/Desktop/vectoraiz.desktop" 2>/dev/null || true
    chmod +x "$HOME/Desktop/vectoraiz.desktop" 2>/dev/null || true
    success "Desktop shortcut created"
fi

# ─── Step 12: Offer systemd service ──────────────────────────────
if command -v systemctl &>/dev/null; then
    echo ""
    echo -e "  ${CYAN}${BOLD}─── Auto-start on boot ────────────────────${NC}"
    echo ""
    echo -e "  Would you like vectorAIz to start automatically when you log in?"
    echo ""
    read -rp "  Enable auto-start? [y/N]: " AUTOSTART

    if [[ "$AUTOSTART" =~ ^[Yy]$ ]]; then
        SYSTEMD_DIR="$HOME/.config/systemd/user"
        mkdir -p "$SYSTEMD_DIR"

        cat > "$SYSTEMD_DIR/vectoraiz.service" <<EOF
[Unit]
Description=vectorAIz — Self-hosted data processing & search
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$INSTALL_DIR
ExecStart=/usr/bin/docker compose -f $COMPOSE_FILE up -d
ExecStop=/usr/bin/docker compose -f $COMPOSE_FILE down

[Install]
WantedBy=default.target
EOF

        systemctl --user daemon-reload
        systemctl --user enable vectoraiz.service
        # Enable lingering so user services start at boot
        loginctl enable-linger "$USER" 2>/dev/null || true
        success "Auto-start enabled (systemd user service)"
    else
        info "Skipping auto-start"
    fi
    echo ""
fi

# ─── Step 13: Open browser ───────────────────────────────────────
print_ready

if command -v xdg-open &>/dev/null; then
    sleep 1
    xdg-open "$URL" 2>/dev/null || true
fi

echo -e "  ${CYAN}Tip:${NC} Launch vectorAIz from your app launcher or: cd ~/vectoraiz && docker compose up -d"
echo -e "  ${CYAN}Tip:${NC} View logs: cd ~/vectoraiz && docker compose -f $COMPOSE_FILE logs -f"
echo -e "  ${CYAN}Tip:${NC} Stop: cd ~/vectoraiz && docker compose -f $COMPOSE_FILE down"
echo ""
