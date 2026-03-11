#!/usr/bin/env bash
# =============================================================================
# vectorAIz Release Script (v2)
# =============================================================================
# Two commands:
#   ./scripts/release.sh rc [patch|minor|major]   — create a release candidate
#   ./scripts/release.sh promote [vX.Y.Z-rc.N]    — promote RC to stable
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

IMAGE="ghcr.io/aidotmarket/vectoraiz"
COMPOSE_FILE="docker-compose.customer.yml"

# ---------------------------------------------------------------------------
# Colors & helpers
# ---------------------------------------------------------------------------
BOLD='\033[1m' GREEN='\033[0;32m' RED='\033[0;31m' CYAN='\033[0;36m' YELLOW='\033[0;33m' RESET='\033[0m'

info()   { echo -e "  ${CYAN}▸${RESET} $*"; }
pass()   { echo -e "  ${GREEN}✔${RESET} $*"; }
header() { echo -e "\n${BOLD}═══ $* ═══${RESET}"; }
die()    { echo -e "\n  ${RED}✘${RESET} $1"; [[ -n "${2:-}" ]] && echo -e "  ${YELLOW}Recovery:${RESET} $2"; echo; exit 1; }

# ---------------------------------------------------------------------------
# Docker binary (OrbStack / standard)
# ---------------------------------------------------------------------------
DOCKER="${DOCKER:-docker}"
if ! command -v "$DOCKER" &>/dev/null; then
  for c in /Users/max/.orbstack/bin/docker /usr/local/bin/docker /opt/homebrew/bin/docker; do
    [[ -x "$c" ]] && { DOCKER="$c"; break; }
  done
fi

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
preflight() {
  header "Pre-flight checks"

  local branch; branch=$(git rev-parse --abbrev-ref HEAD)
  [[ "$branch" == "main" ]] || die "Not on main (on '$branch')." "git checkout main"
  pass "On main branch"

  git diff --quiet HEAD 2>/dev/null || die "Uncommitted changes." "git stash or git commit"
  git diff --cached --quiet 2>/dev/null || die "Staged changes." "git commit or git reset HEAD"
  pass "Working tree clean"

  "$DOCKER" version &>/dev/null || die "Docker not available." "Start Docker Desktop / OrbStack"
  pass "Docker available"

  command -v gh &>/dev/null || die "gh CLI not found." "brew install gh && gh auth login"
  gh auth status &>/dev/null || die "gh not authenticated." "gh auth login"
  pass "gh CLI authenticated"

  # GHCR access
  if ! "$DOCKER" manifest inspect "$IMAGE:latest" &>/dev/null; then
    info "Logging into GHCR..."
    local token="${GITHUB_TOKEN:-}"
    [[ -z "$token" ]] && token=$(doppler secrets get GITHUB_TOKEN --plain -p ai-market -c dev_personal 2>/dev/null || echo "")
    [[ -z "$token" ]] && die "No GITHUB_TOKEN for GHCR." "Set GITHUB_TOKEN env var"
    echo "$token" | "$DOCKER" login ghcr.io -u aidotmarket --password-stdin || die "GHCR login failed."
    pass "Logged into GHCR"
  else
    pass "GHCR accessible"
  fi
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
latest_stable_tag() {
  git tag -l 'v*' --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1 || true
}

update_compose() {
  local ver="$1"
  sed -i '' "s/VECTORAIZ_VERSION:-v[^}]*/VECTORAIZ_VERSION:-v${ver}/" "$COMPOSE_FILE"
  grep -q "VECTORAIZ_VERSION:-v${ver}" "$COMPOSE_FILE" || die "sed failed to update $COMPOSE_FILE"
  pass "Compose updated → v${ver}"
}

commit_tag_push() {
  local ver="$1" msg="$2" prerelease="${3:-false}"

  git add "$COMPOSE_FILE"
  if ! git diff --cached --quiet; then
    git commit -m "$msg"
    pass "Committed: $msg"
  fi

  git tag -a "v${ver}" -m "Release v${ver}"
  pass "Tagged v${ver}"

  git push origin main "v${ver}"
  pass "Pushed commit + tag"
}

wait_for_image() {
  local tag="$1" max=600 interval=15 elapsed=0
  header "Waiting for $IMAGE:${tag}"
  while (( elapsed < max )); do
    if "$DOCKER" manifest inspect "$IMAGE:${tag}" &>/dev/null; then
      pass "Image $IMAGE:${tag} available"
      return 0
    fi
    printf "\r  ⏳ %d:%02d elapsed " $((elapsed/60)) $((elapsed%60))
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done
  echo
  die "Timed out (10 min) waiting for $IMAGE:${tag}" \
      "Check GitHub Actions: gh run list --workflow=docker-publish.yml"
}

# ---------------------------------------------------------------------------
# rc [patch|minor|major]
# ---------------------------------------------------------------------------
cmd_rc() {
  local bump="${1:-patch}"
  [[ "$bump" =~ ^(patch|minor|major)$ ]] || die "Invalid bump: '$bump'. Use patch|minor|major."

  preflight

  header "RC version resolution"

  local stable_tag; stable_tag=$(latest_stable_tag)
  local current="${stable_tag#v}"; [[ -z "$current" ]] && current="0.0.0"
  local major minor patch
  IFS='.' read -r major minor patch <<< "$current"

  local next
  case "$bump" in
    patch) next="$major.$minor.$((patch + 1))" ;;
    minor) next="$major.$((minor + 1)).0" ;;
    major) next="$((major + 1)).0.0" ;;
  esac

  # Find highest existing RC for this version
  local highest=0
  while IFS= read -r t; do
    [[ -z "$t" ]] && continue
    local n="${t##*-rc.}"
    (( n > highest )) 2>/dev/null && highest="$n"
  done < <(git tag -l "v${next}-rc.*")

  local rc_num=$((highest + 1))
  local ver="${next}-rc.${rc_num}"

  git rev-parse "v${ver}" &>/dev/null && die "Tag v${ver} already exists."

  pass "Current stable: v${current}"
  pass "New RC:         v${ver}"

  # Update compose, commit, tag, push
  update_compose "$ver"
  commit_tag_push "$ver" "chore: release v${ver}"

  # GitHub prerelease
  gh release create "v${ver}" --prerelease --title "v${ver}" --generate-notes \
    || die "Failed to create GitHub pre-release." "gh release create v${ver} --prerelease --generate-notes"
  pass "GitHub pre-release created"

  echo -e "\n${GREEN}${BOLD}RC RELEASED: v${ver}${RESET}"
  echo -e "  Image: ${IMAGE}:v${ver}"
  echo -e "  Tag:   v${ver}\n"
}

# ---------------------------------------------------------------------------
# promote [vX.Y.Z-rc.N]
# ---------------------------------------------------------------------------
cmd_promote() {
  local rc_tag="${1:-}"

  preflight

  header "Promote RC → stable"

  # Find or validate RC tag
  if [[ -z "$rc_tag" ]]; then
    rc_tag=$(git tag -l 'v*-rc.*' --sort=-v:refname | head -1)
    [[ -z "$rc_tag" ]] && die "No RC tags found." "Create one: ./scripts/release.sh rc"
    info "Latest RC: $rc_tag"
  fi
  [[ "$rc_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-rc\.[0-9]+$ ]] \
    || die "Invalid RC format: '$rc_tag'" "Expected: v1.2.3-rc.1"

  local ver="${rc_tag#v}"; ver="${ver%-rc.*}"
  git rev-parse "v${ver}" &>/dev/null && die "Stable tag v${ver} already exists."

  pass "RC:     $rc_tag"
  pass "Stable: v${ver}"

  # Update compose to stable version, commit on main, tag THAT commit
  update_compose "$ver"
  commit_tag_push "$ver" "chore: release v${ver}" false

  # Poll for image
  wait_for_image "v${ver}"

  # Smoke: pull the image
  info "Pulling image..."
  "$DOCKER" pull "$IMAGE:v${ver}" &>/dev/null && pass "docker pull OK" || die "docker pull failed"

  # GitHub release
  gh release create "v${ver}" --latest --title "v${ver}" --generate-notes \
    || die "Failed to create GitHub release." "gh release create v${ver} --latest --generate-notes"
  pass "GitHub release created (latest)"

  echo -e "\n${GREEN}${BOLD}STABLE RELEASED: v${ver}${RESET}  (promoted from $rc_tag)"
  echo -e "  Image: ${IMAGE}:v${ver}"
  echo -e "  Tag:   v${ver}\n"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
case "${1:-}" in
  rc)      cmd_rc "${2:-patch}" ;;
  promote) cmd_promote "${2:-}" ;;
  *)       die "Usage: release.sh <rc [patch|minor|major] | promote [vX.Y.Z-rc.N]>" ;;
esac
