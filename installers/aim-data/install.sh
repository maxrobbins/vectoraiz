#!/usr/bin/env bash
set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

info() { echo -e "  ${CYAN}▸${NC} $*"; }
pass() { echo -e "  ${GREEN}✔${NC} $*"; }
warn() { echo -e "  ${YELLOW}!${NC} $*"; }
die()  { echo -e "\n  ${RED}✘${NC} $*\n"; exit 1; }

REPO_RAW="https://raw.githubusercontent.com/aidotmarket/vectoraiz/main"
COMPOSE_URL="${REPO_RAW}/docker-compose.aim-data.yml"
IMAGE="ghcr.io/aidotmarket/vectoraiz:latest"
INSTALL_DIR="${AIM_DATA_INSTALL_DIR:-$HOME/aim-data}"
COMPOSE_FILE="docker-compose.aim-data.yml"

generate_secret() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 16
  else
    date +%s%N | shasum | awk '{print $1}' | cut -c1-32
  fi
}

echo
echo -e "${CYAN}${BOLD}  ⚡ AIM-Data Installer${NC}"
echo

if ! command -v docker >/dev/null 2>&1; then
  die "Docker is not installed. Install Docker Desktop or Docker Engine, then re-run this script.
      https://docs.docker.com/get-docker/"
fi

if ! docker info >/dev/null 2>&1; then
  die "Docker is installed but the daemon is not running. Start Docker Desktop / the service, then re-run."
fi
pass "Docker is ready"

mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"
pass "Install dir: $INSTALL_DIR"

info "Downloading ${COMPOSE_FILE}..."
if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$COMPOSE_URL" -o "$COMPOSE_FILE"
elif command -v wget >/dev/null 2>&1; then
  wget -q "$COMPOSE_URL" -O "$COMPOSE_FILE"
else
  die "Neither curl nor wget is available."
fi
pass "Downloaded compose file"

if [[ ! -f .env ]]; then
  cat > .env <<EOF
# AIM-Data configuration
POSTGRES_PASSWORD=$(generate_secret)
VECTORAIZ_SECRET_KEY=$(generate_secret)
VECTORAIZ_VERSION=latest
VECTORAIZ_CHANNEL=aim-data
AIM_DATA_PORT=8080
VECTORAIZ_MODE=standalone
EOF
  pass "Generated .env"
else
  info ".env already exists — keeping it"
fi

info "Pulling ${IMAGE}..."
docker pull "$IMAGE" || die "Failed to pull ${IMAGE}"
pass "Image pulled"

info "Starting AIM-Data..."
docker compose -f "$COMPOSE_FILE" up -d || die "docker compose up failed"
pass "Containers started"

PORT=$(grep '^AIM_DATA_PORT=' .env | cut -d= -f2)
PORT="${PORT:-8080}"
URL="http://localhost:${PORT}"

echo
echo -e "${GREEN}${BOLD}  ✅ AIM-Data is running${NC}"
echo -e "     URL:   ${CYAN}${URL}${NC}"
echo -e "     Dir:   ${INSTALL_DIR}"
echo -e "     Logs:  docker compose -f ${INSTALL_DIR}/${COMPOSE_FILE} logs -f"
echo
