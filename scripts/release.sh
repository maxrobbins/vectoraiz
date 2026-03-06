#!/usr/bin/env bash
# =============================================================================
# vectorAIz — Hardened Release Script
# =============================================================================
# Single source of truth: git tags. Image built by GitHub Actions on tag push.
#
# Usage:
#   ./scripts/release.sh patch          # RC from patch bump (1.20.26 → 1.20.27-rc.1)
#   ./scripts/release.sh minor          # RC from minor bump (1.20.26 → 1.21.0-rc.1)
#   ./scripts/release.sh major          # RC from major bump (1.20.26 → 2.0.0-rc.1)
#   ./scripts/release.sh rc             # RC from patch bump (same as 'patch')
#   ./scripts/release.sh promote        # Promote latest RC to stable (requires approval)
#   ./scripts/release.sh promote v1.20.27-rc.3  # Promote specific RC to stable
#
# See docs/RELEASING.md for full documentation and recovery procedures.
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

IMAGE="ghcr.io/aidotmarket/vectoraiz"
COMPOSE_FILE="docker-compose.customer.yml"
GITHUB_RAW="https://raw.githubusercontent.com/aidotmarket/vectoraiz/main"

# -----------------------------------------------------------------------------
# Colors and output helpers
# -----------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

pass()  { echo -e "  ${GREEN}✔${RESET} $*"; }
fail()  { echo -e "  ${RED}✘${RESET} $*"; }
info()  { echo -e "  ${CYAN}▸${RESET} $*"; }
warn()  { echo -e "  ${YELLOW}⚠${RESET} $*"; }
header() { echo -e "\n${BOLD}═══ $* ═══${RESET}"; }

die() {
  echo ""
  fail "$1"
  if [ -n "${2:-}" ]; then
    echo -e "  ${YELLOW}Recovery:${RESET} $2"
  fi
  echo ""
  exit 1
}

# -----------------------------------------------------------------------------
# Resolve docker binary (OrbStack or standard)
# -----------------------------------------------------------------------------
DOCKER="${DOCKER_BIN:-docker}"
if ! command -v "$DOCKER" &>/dev/null; then
  for candidate in /Users/max/.orbstack/bin/docker /usr/local/bin/docker /opt/homebrew/bin/docker; do
    if [ -x "$candidate" ]; then DOCKER="$candidate"; break; fi
  done
fi

# =============================================================================
# PRE-FLIGHT CHECKS
# =============================================================================
preflight() {
  header "Pre-flight checks"

  # --- On main branch ---
  local branch
  branch=$(git rev-parse --abbrev-ref HEAD)
  if [ "$branch" != "main" ]; then
    die "Not on main branch (currently on '$branch')." \
        "Run: git checkout main"
  fi
  pass "On main branch"

  # --- Clean working tree ---
  if ! git diff --quiet HEAD 2>/dev/null; then
    fail "Uncommitted changes detected:"
    git status --short
    die "Working tree is not clean." \
        "Run: git stash  or  git commit"
  fi
  if ! git diff --cached --quiet 2>/dev/null; then
    die "Staged but uncommitted changes detected." \
        "Run: git commit  or  git reset HEAD"
  fi
  pass "Working tree clean"

  # --- Docker CLI available ---
  if ! "$DOCKER" version &>/dev/null; then
    die "Docker CLI not available or daemon not running." \
        "Start Docker Desktop / OrbStack, or set DOCKER_BIN env var."
  fi
  pass "Docker CLI available ($DOCKER)"

  # --- gh CLI available and authenticated ---
  if ! command -v gh &>/dev/null; then
    die "GitHub CLI (gh) not found." \
        "Install: brew install gh && gh auth login"
  fi
  if ! gh auth status &>/dev/null; then
    die "GitHub CLI not authenticated." \
        "Run: gh auth login"
  fi
  pass "GitHub CLI authenticated"

  # --- GHCR auth (needed for manifest inspect later) ---
  if ! "$DOCKER" manifest inspect "$IMAGE:latest" &>/dev/null; then
    info "Logging into GHCR..."
    if [ -z "${GITHUB_TOKEN:-}" ]; then
      GITHUB_TOKEN=$(doppler secrets get GITHUB_TOKEN --plain -p ai-market -c dev_personal 2>/dev/null || echo "")
    fi
    if [ -z "${GITHUB_TOKEN:-}" ]; then
      die "Cannot authenticate to GHCR. No GITHUB_TOKEN found." \
          "Set GITHUB_TOKEN env var or run: doppler secrets get GITHUB_TOKEN --plain"
    fi
    echo "$GITHUB_TOKEN" | "$DOCKER" login ghcr.io -u aidotmarket --password-stdin || \
      die "GHCR login failed." "Check your GITHUB_TOKEN has packages:read scope."
    pass "Logged into GHCR"
  else
    pass "GHCR accessible"
  fi
}

# =============================================================================
# VERSION RESOLUTION
# =============================================================================
resolve_version() {
  header "Version resolution"

  local bump="${1:?Usage: release.sh <version|patch|minor|major>}"

  # Get current version from latest git tag
  local current_tag
  current_tag=$(git tag -l 'v*' --sort=-v:refname | { grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' || true; } | head -1)
  local current="${current_tag#v}"
  if [ -z "$current" ]; then
    current="0.0.0"
  fi

  local major minor patch
  IFS='.' read -r major minor patch <<< "$current"

  case "$bump" in
    patch) VERSION="$major.$minor.$((patch + 1))" ;;
    minor) VERSION="$major.$((minor + 1)).0" ;;
    major) VERSION="$((major + 1)).0.0" ;;
    *)
      # Validate explicit version format
      if ! echo "$bump" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
        die "Invalid version format: '$bump'" \
            "Use semver (e.g., 1.17.0) or: patch, minor, major"
      fi
      VERSION="$bump"
      ;;
  esac

  # Check tag doesn't already exist
  if git rev-parse "v$VERSION" &>/dev/null; then
    die "Tag v$VERSION already exists." \
        "Choose a different version, or delete the tag: git tag -d v$VERSION && git push origin :refs/tags/v$VERSION"
  fi

  pass "Current version: ${current:-none}"
  pass "New version:     $VERSION"

  echo ""
  echo -e "${BOLD}╔═══════════════════════════════════════════════════╗${RESET}"
  echo -e "${BOLD}║  Releasing vectorAIz v${VERSION}$(printf '%*s' $((26 - ${#VERSION})) '')║${RESET}"
  echo -e "${BOLD}╚═══════════════════════════════════════════════════╝${RESET}"
}

# =============================================================================
# STEP 1: Update docker-compose.customer.yml
# =============================================================================
step1_update_compose() {
  header "Step 1: Update $COMPOSE_FILE"

  info "Setting VECTORAIZ_VERSION default to $VERSION"

  # Perform the substitution (macOS sed vs GNU sed)
  if sed --version 2>/dev/null | grep -q GNU; then
    sed -i "s/VECTORAIZ_VERSION:-[^}]*/VECTORAIZ_VERSION:-v${VERSION}/" "$COMPOSE_FILE"
  else
    sed -i '' "s/VECTORAIZ_VERSION:-[^}]*/VECTORAIZ_VERSION:-v${VERSION}/" "$COMPOSE_FILE"
  fi

  # VERIFY: grep the file and confirm
  if ! grep -q "VECTORAIZ_VERSION:-v${VERSION}" "$COMPOSE_FILE"; then
    die "Failed to update $COMPOSE_FILE — version $VERSION not found after sed." \
        "Manually edit $COMPOSE_FILE and set VECTORAIZ_VERSION default to $VERSION."
  fi

  pass "$COMPOSE_FILE updated to $VERSION"
}

# =============================================================================
# STEP 2: Commit + push
# =============================================================================
step2_commit_push() {
  header "Step 2: Commit + push"

  git add "$COMPOSE_FILE"

  # Check there's actually something to commit
  if git diff --cached --quiet; then
    warn "$COMPOSE_FILE unchanged (version was already $VERSION?)"
    warn "Skipping commit — nothing to push"
    return 0
  fi

  git commit -m "Release v${VERSION}: update compose default"
  pass "Committed"

  git push origin main
  pass "Pushed to origin/main"

  # VERIFY: raw GitHub content shows new version (with retries for CDN propagation)
  info "Verifying compose file on GitHub (CDN may take a moment)..."
  local raw_url="${GITHUB_RAW}/${COMPOSE_FILE}"
  local attempt
  for attempt in 1 2 3; do
    sleep 5
    local content
    content=$(curl -fsSL "$raw_url" 2>/dev/null || echo "")
    if echo "$content" | grep -q "VECTORAIZ_VERSION:-v${VERSION}"; then
      pass "GitHub raw content verified (attempt $attempt)"
      return 0
    fi
    if [ "$attempt" -lt 3 ]; then
      warn "Attempt $attempt: version not yet visible on GitHub, retrying in 5s..."
    fi
  done

  die "GitHub raw content does not show version $VERSION after 3 attempts." \
      "Check ${raw_url} manually. The commit was pushed — you can continue by re-running the script (it will skip the commit)."
}

# =============================================================================
# STEP 3: Tag + push tag
# =============================================================================
step3_tag() {
  header "Step 3: Tag + push tag"

  git tag -a "v$VERSION" -m "Release $VERSION"
  pass "Created tag v$VERSION"

  git push origin "v$VERSION"
  pass "Pushed tag v$VERSION to origin"

  # VERIFY: tag exists on remote
  info "Verifying tag on GitHub..."
  local attempt
  for attempt in 1 2 3; do
    if gh release view "v$VERSION" &>/dev/null || git ls-remote --tags origin "v$VERSION" | grep -q "v$VERSION"; then
      pass "Tag v$VERSION confirmed on remote"
      return 0
    fi
    if [ "$attempt" -lt 3 ]; then
      info "Waiting for tag/release to appear (attempt $attempt)..."
      sleep 5
    fi
  done

  # Tag was pushed — even if release isn't created yet by Actions, the tag exists
  if git ls-remote --tags origin | grep -q "refs/tags/v$VERSION"; then
    pass "Tag v$VERSION exists on remote (release may still be in progress)"
  else
    die "Tag v$VERSION not found on remote." \
        "Run: git push origin v$VERSION"
  fi
}

# =============================================================================
# STEP 4: Wait for GHCR image
# =============================================================================
step4_wait_for_image() {
  header "Step 4: Wait for GHCR image"

  info "Waiting for ghcr.io/aidotmarket/vectoraiz:v${VERSION} ..."
  info "GitHub Actions builds the image on tag push. This can take several minutes."
  echo ""

  local max_wait=1800  # 30 minutes
  local interval=30
  local elapsed=0

  while [ "$elapsed" -lt "$max_wait" ]; do
    if "$DOCKER" manifest inspect "$IMAGE:v$VERSION" &>/dev/null; then
      echo ""
      pass "Image $IMAGE:v$VERSION is available on GHCR"
      return 0
    fi

    local remaining=$(( (max_wait - elapsed) / 60 ))
    printf "\r  ⏳ Waiting... %d:%02d elapsed (%d min remaining)  " \
      $((elapsed / 60)) $((elapsed % 60)) "$remaining"
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  echo ""
  die "Timed out after 30 minutes waiting for $IMAGE:v$VERSION" \
      "Check GitHub Actions: gh run list --workflow=docker-publish.yml. The tag v$VERSION was already pushed — once the image builds, the release is complete."
}

# =============================================================================
# STEP 5: Create GitHub Release
# =============================================================================
step5_create_release() {
  header "Step 5: Create GitHub Release"

  info "Creating GitHub Release for v$VERSION..."

  if gh release view "v$VERSION" &>/dev/null; then
    pass "GitHub Release v$VERSION already exists (created by CI)"
    # Force --latest in case CI created releases out of order
    gh release edit "v$VERSION" --latest 2>/dev/null || true
    pass "Marked v$VERSION as latest"
    return 0
  fi

  gh release create "v$VERSION" \
    --title "v$VERSION" \
    --generate-notes \
    --latest \
    || die "Failed to create GitHub Release for v$VERSION." \
           "Run manually: gh release create v\$VERSION --title v\$VERSION --generate-notes"

  pass "GitHub Release v$VERSION created"

  # VERIFY: install script will now resolve to this version
  info "Verifying /releases/latest resolves to v$VERSION..."
  local latest
  latest=$(curl -fsSL "https://api.github.com/repos/aidotmarket/vectoraiz/releases/latest" 2>/dev/null | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')
  if [ "$latest" = "v$VERSION" ]; then
    pass "GitHub /releases/latest resolves to v$VERSION"
  else
    warn "/releases/latest shows '$latest' instead of v$VERSION - may take a moment to propagate"
  fi
}

# =============================================================================
# STEP 6: Post-release smoke test
# =============================================================================
step6_smoke_test() {
  header "Step 6: Post-release smoke test"

  # 5a: Install script compose file resolves to new version
  info "Checking install script compose URL..."
  local compose_content
  compose_content=$(curl -fsSL "${GITHUB_RAW}/${COMPOSE_FILE}" 2>/dev/null || echo "")
  if echo "$compose_content" | grep -q "VECTORAIZ_VERSION:-v${VERSION}"; then
    pass "Install compose resolves to v$VERSION"
  else
    warn "Install compose does not yet show v$VERSION (CDN cache). Customers pulling fresh will get the right version once CDN updates."
  fi

  # 5b: :latest digest matches :v${VERSION} digest
  info "Checking :latest tag matches v$VERSION..."
  local version_digest latest_digest
  version_digest=$("$DOCKER" manifest inspect "$IMAGE:v$VERSION" 2>/dev/null | grep -o '"sha256:[a-f0-9]*"' | head -1 || echo "unknown")
  latest_digest=$("$DOCKER" manifest inspect "$IMAGE:latest" 2>/dev/null | grep -o '"sha256:[a-f0-9]*"' | head -1 || echo "unknown")

  if [ "$version_digest" = "unknown" ]; then
    warn "Could not inspect v$VERSION manifest for digest comparison"
  elif [ "$latest_digest" = "unknown" ]; then
    warn "Could not inspect :latest manifest — Actions may not have updated it yet"
  elif [ "$version_digest" = "$latest_digest" ]; then
    pass ":latest digest matches v$VERSION"
  else
    warn ":latest digest does not match v$VERSION yet. Actions may still be updating the :latest tag."
    info "v$VERSION digest: $version_digest"
    info ":latest  digest: $latest_digest"
  fi
}

# =============================================================================
# SUMMARY
# =============================================================================
print_summary() {
  local install_url="${1:-curl -fsSL ${GITHUB_RAW}/install.sh | bash}"
  local label="${2:-RELEASE COMPLETE}"

  echo ""
  echo -e "${GREEN}${BOLD}╔═══════════════════════════════════════════════════════════════╗${RESET}"
  printf "${GREEN}${BOLD}║${RESET}  %-60s${GREEN}${BOLD}║${RESET}\n" "$label"
  echo -e "${GREEN}${BOLD}╠═══════════════════════════════════════════════════════════════╣${RESET}"
  printf "${GREEN}${BOLD}║${RESET}  Version:  ${BOLD}%-49s${RESET}${GREEN}${BOLD}║${RESET}\n" "v${VERSION}"
  printf "${GREEN}${BOLD}║${RESET}  Image:    ${BOLD}%-49s${RESET}${GREEN}${BOLD}║${RESET}\n" "${IMAGE}:v${VERSION}"
  printf "${GREEN}${BOLD}║${RESET}  Tag:      ${BOLD}%-49s${RESET}${GREEN}${BOLD}║${RESET}\n" "v${VERSION}"
  echo -e "${GREEN}${BOLD}╠═══════════════════════════════════════════════════════════════╣${RESET}"
  printf "${GREEN}${BOLD}║${RESET}  Install:  %-49s${GREEN}${BOLD}║${RESET}\n" ""
  printf "${GREEN}${BOLD}║${RESET}  %-60s${GREEN}${BOLD}║${RESET}\n" "$install_url"
  echo -e "${GREEN}${BOLD}╚═══════════════════════════════════════════════════════════════╝${RESET}"
  echo ""
}

# =============================================================================
# HELPER: Get current stable version (latest tag matching vX.Y.Z without -rc)
# =============================================================================
get_current_stable() {
  local tag
  tag=$(git tag -l 'v*' --sort=-v:refname | { grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' || true; } | head -1)
  echo "${tag#v}"
}

# =============================================================================
# FLOW: Stable release (patch/minor/major/explicit — existing behavior)
# =============================================================================
stable_flow() {
  resolve_version "${1:?Usage: release.sh <version|patch|minor|major>}"
  step1_update_compose
  step2_commit_push
  step3_tag
  step4_wait_for_image
  step5_create_release
  step6_smoke_test
  print_summary "curl -fsSL https://get.vectoraiz.com | bash" "STABLE RELEASE COMPLETE"
}

# =============================================================================
# FLOW: Release Candidate
# =============================================================================
rc_flow() {
  local bump="${1:-patch}"

  header "RC version resolution"

  local current
  current=$(get_current_stable)
  if [ -z "$current" ]; then current="0.0.0"; fi

  local major minor patch
  IFS='.' read -r major minor patch <<< "$current"

  local next_version
  case "$bump" in
    patch) next_version="$major.$minor.$((patch + 1))" ;;
    minor) next_version="$major.$((minor + 1)).0" ;;
    major) next_version="$((major + 1)).0.0" ;;
  esac

  # Find highest existing RC for this version
  local highest_rc=0
  local rc_tag
  while IFS= read -r rc_tag; do
    [ -z "$rc_tag" ] && continue
    local rc_num="${rc_tag##*-rc.}"
    if [ "$rc_num" -gt "$highest_rc" ] 2>/dev/null; then
      highest_rc="$rc_num"
    fi
  done < <(git tag -l "v${next_version}-rc.*")

  local rc_number=$((highest_rc + 1))
  VERSION="${next_version}-rc.${rc_number}"

  # Check tag doesn't already exist (safety)
  if git rev-parse "v$VERSION" &>/dev/null; then
    die "Tag v$VERSION already exists." \
        "This shouldn't happen. Delete it with: git tag -d v$VERSION && git push origin :refs/tags/v$VERSION"
  fi

  pass "Current stable: v${current}"
  pass "Next version:   ${next_version}"
  pass "RC number:      ${rc_number}"
  pass "RC tag:         v${VERSION}"

  echo ""
  echo -e "${BOLD}╔═══════════════════════════════════════════════════╗${RESET}"
  echo -e "${BOLD}║  Release Candidate: v${VERSION}$(printf '%*s' $((28 - ${#VERSION})) '')║${RESET}"
  echo -e "${BOLD}╚═══════════════════════════════════════════════════╝${RESET}"

  # Tag from HEAD of main — no compose update, no commit
  step3_tag
  step4_wait_for_image

  # Create GitHub Release with --prerelease
  header "Create GitHub Pre-release"
  if gh release view "v$VERSION" &>/dev/null; then
    pass "GitHub Release v$VERSION already exists"
  else
    gh release create "v$VERSION" \
      --prerelease \
      --title "v$VERSION" \
      --generate-notes \
      || die "Failed to create GitHub pre-release for v$VERSION." \
             "Run manually: gh release create v$VERSION --prerelease --title v$VERSION --generate-notes"
    pass "GitHub pre-release v$VERSION created"
  fi

  print_summary "curl -fsSL https://get.vectoraiz.com/rc | bash" "RC RELEASE COMPLETE"
}

# =============================================================================
# FLOW: Promote RC to stable
# =============================================================================
promote_flow() {
  local rc_tag="${1:-}"

  header "Promote RC to stable"

  if [ -z "$rc_tag" ]; then
    # Find latest RC tag automatically
    rc_tag=$(git tag -l 'v*-rc.*' --sort=-v:refname | head -1)
    if [ -z "$rc_tag" ]; then
      die "No RC tags found." \
          "Create one first with: ./scripts/release.sh rc"
    fi
    info "Auto-detected latest RC: $rc_tag"
  fi

  # Validate format
  if ! echo "$rc_tag" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+-rc\.[0-9]+$'; then
    die "Invalid RC tag format: '$rc_tag'" \
        "Expected format: v1.20.27-rc.1"
  fi

  # Extract stable version from RC tag (e.g., v1.20.27-rc.3 → 1.20.27)
  VERSION="${rc_tag#v}"
  VERSION="${VERSION%-rc.*}"

  pass "RC tag:          $rc_tag"
  pass "Stable version:  v$VERSION"

  # Check stable tag doesn't already exist
  if git rev-parse "v$VERSION" &>/dev/null; then
    die "Stable tag v$VERSION already exists." \
        "This RC may have already been promoted."
  fi

  # Validate RC Docker image exists on GHCR
  info "Verifying RC image exists on GHCR..."
  if ! "$DOCKER" manifest inspect "$IMAGE:$rc_tag" &>/dev/null; then
    die "RC image $IMAGE:$rc_tag not found on GHCR." \
        "Wait for the RC build to complete, or rebuild with: ./scripts/release.sh rc"
  fi
  pass "RC image $IMAGE:$rc_tag exists on GHCR"

  # Get the commit SHA that the RC tag points to
  local rc_sha
  rc_sha=$(git rev-list -n 1 "$rc_tag" 2>/dev/null) || \
    die "Cannot resolve commit for $rc_tag" "Fetch tags: git fetch --tags"
  pass "RC commit: ${rc_sha:0:12}"

  echo ""
  echo -e "${BOLD}╔═══════════════════════════════════════════════════╗${RESET}"
  echo -e "${BOLD}║  Promoting $rc_tag → v${VERSION}$(printf '%*s' $((25 - ${#rc_tag} - ${#VERSION})) '')║${RESET}"
  echo -e "${BOLD}╚═══════════════════════════════════════════════════╝${RESET}"

  # Update compose file, commit, push
  step1_update_compose
  step2_commit_push

  # Create stable tag from the SAME commit the RC pointed to
  header "Create stable tag from RC commit"
  git tag -a "v$VERSION" "$rc_sha" -m "Release $VERSION (promoted from $rc_tag)"
  pass "Created tag v$VERSION from $rc_sha"
  git push origin "v$VERSION"
  pass "Pushed tag v$VERSION to origin"

  step4_wait_for_image

  # Create stable GitHub Release (--latest, NOT prerelease)
  header "Create stable GitHub Release"
  if gh release view "v$VERSION" &>/dev/null; then
    pass "GitHub Release v$VERSION already exists"
    gh release edit "v$VERSION" --latest 2>/dev/null || true
  else
    gh release create "v$VERSION" \
      --title "v$VERSION" \
      --generate-notes \
      --latest \
      || die "Failed to create GitHub Release for v$VERSION." \
             "Run manually: gh release create v$VERSION --title v$VERSION --generate-notes --latest"
    pass "GitHub Release v$VERSION created"
  fi

  step6_smoke_test
  print_summary "curl -fsSL https://get.vectoraiz.com | bash" "STABLE RELEASE COMPLETE (promoted from $rc_tag)"
}

# =============================================================================
# MAIN
# =============================================================================
main() {
  local cmd="${1:-}"

  case "$cmd" in
    rc)
      preflight
      rc_flow
      ;;
    promote)
      preflight
      promote_flow "${2:-}"
      ;;
    patch|minor|major)
      preflight
      rc_flow "$cmd"
      ;;
    "")
      die "Usage: release.sh <rc|promote|patch|minor|major>" \
          "All builds create RCs. Use 'promote' to release stable. See docs/RELEASING.md."
      ;;
    *)
      die "Usage: release.sh <rc|promote|patch|minor|major>" \
          "All builds create RCs. Use 'promote' to release stable."
      ;;
  esac
}

main "$@"
