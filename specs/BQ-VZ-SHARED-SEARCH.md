# BQ-VZ-SHARED-SEARCH: Shared Search Portal

**Status:** Planned (Gate 1 pending)
**Priority:** P1
**Estimated:** 16h (Phase 1: 10h, Phase 2: 6h)
**Dependencies:** None
**Created:** S243
**Triggered by:** Real user feedback — "How do I let other users in my office use vectorAIz to search out files?"

---

## Problem

A VZ user wants coworkers to search their data. Today there's no clean answer:
- **Share localhost URL** → exposes full admin UI (upload, delete, config)
- **Set up MCP tokens** → too technical for most users
- **Install VZ per person** → unnecessary, they just want to search

The user's question is simple. The product answer should be simple too.

## Solution

A **shared search portal** — a lightweight, read-only web UI served by VZ at `/portal`. Separate from the admin interface. Three progressive access tiers that map to VZ's pricing tiers.

## Access Tiers

### Open (Community tier — free)
- No authentication. Anyone with the URL can search.
- Best for: small trusted teams, demos, personal use.
- Admin enables via single toggle in VZ Settings.
- Risk mitigation: per-IP rate limiting.

### PIN (Team tier — paid)
- Shared PIN code (4-8 digits), like a WiFi password.
- No per-user tracking — just a gate to prevent casual access.
- Admin sets and rotates PIN in VZ Settings.
- Best for: office teams, small companies, shared workspaces.

### SSO/OIDC (Enterprise tier — paid)
- Authenticate via company Identity Provider (Okta, Azure AD, Google Workspace, etc.).
- Per-user access logs and audit trail.
- Requires VZ Enterprise license.
- Best for: regulated industries, large teams, compliance requirements.

## Portal UI

The portal is a **separate route** (`/portal`) with its own layout — no sidebar, no admin navigation. It's what a coworker sees when they open the shared URL.

### Components
1. **Search bar** — natural language query input (uses existing VZ search/RAG pipeline)
2. **Results table** — matching rows with relevance scores, column highlighting
3. **Dataset selector** — only datasets the admin has marked as portal-visible
4. **allAI chat** (optional) — if VZ has allAI configured, portal users can ask questions. Uses buyer-role persona with read-only tool access.
5. **Footer** — "Powered by vectorAIz" with link to vectoraiz.com

### What portal users CANNOT do
- Upload or delete files
- Access SQL query interface
- Change settings or configuration
- See other users or admin controls
- Access the full VZ admin UI

## Admin Controls (VZ Settings)

New "Shared Access" section in VZ Settings page:

1. **Portal toggle** — Enable/disable shared search portal (default: off)
2. **Access tier** — Select Open / PIN / SSO
3. **PIN management** — Set PIN, rotate PIN, show PIN (PIN tier only)
4. **SSO configuration** — OIDC issuer, client ID, client secret (SSO tier only)
5. **Dataset visibility** — Checkboxes for which datasets appear on portal (default: all)
6. **Shareable URL** — Auto-detected URL with copy button:
   - LAN IP detected automatically (e.g., `http://192.168.1.50:8100/portal`)
   - If public tunnel is running, show tunnel URL
   - QR code for easy mobile/in-office sharing

## Technical Design

### Backend
- **New routes:** `/api/portal/search`, `/api/portal/datasets`, `/api/portal/auth`
- **Middleware:** `PortalAuthMiddleware` — checks access tier, validates PIN or OIDC token
- **Separate from admin routes** — `/api/portal/*` has its own auth stack
- **PIN storage:** bcrypt hash in VZ config (not plaintext)
- **Rate limiting:** Per-IP limits on all portal endpoints (configurable, default 60 req/min)

### Frontend
- **Route:** `/portal` with dedicated `PortalLayout` (no sidebar, no admin nav)
- **Minimal bundle** — only search components, no admin code loaded
- **PIN gate:** Simple PIN entry screen if PIN tier active
- **SSO redirect:** Standard OIDC redirect flow if SSO tier active

### allAI in Portal
- Uses existing allAI copilot infrastructure
- **Persona:** Buyer-role from BQ-ALLAI-PERSONAS
- **Tools:** Read-only subset only (search, preview_rows, list_datasets)
- **No destructive tools** — delete, upload, config tools stripped from portal context

## Phases

### Phase 1: Open + PIN (10h)
- Portal backend routes with tier-aware auth
- Portal frontend (search, results, dataset selector)
- Admin controls in Settings (toggle, tier, PIN, dataset visibility, URL display)
- LAN IP auto-detection
- Per-IP rate limiting
- Tests: portal access, PIN validation, dataset filtering, rate limits

### Phase 2: SSO + Audit (6h)
- OIDC integration (issuer discovery, auth flow, token validation)
- Per-user session management
- Audit log (who searched what, when)
- Enterprise license gating
- SSO config UI in Settings
- Tests: OIDC flow, audit logging, license check

## Revenue Connection

| Tier | VZ License | Price Signal |
|------|-----------|-------------|
| Open | Community (free) | "Let your team try it" |
| PIN | Team (paid) | "Simple access control for your office" |
| SSO | Enterprise (paid) | "Your IT team will love this" |

The natural progression: user starts with Open for a demo → realizes they need a PIN so random people can't search → realizes they need SSO for compliance → Enterprise upsell complete.

## Key Principles

1. **Zero friction first** — Open tier: 1 toggle, 1 URL. That's it.
2. **Read-only always** — portal users can never modify anything
3. **Non-custodial** — data stays on VZ, no ai.market proxy
4. **Progressive security** — security grows with the team
5. **allAI enhances but isn't required** — portal works without allAI key

## allAI Response Guidance

When a user asks "how do I let others search?", allAI should:
1. Ask: "Would you like to enable the shared search portal? Your coworkers can search your data from their browser — no installation needed."
2. If yes: enable portal, show the shareable URL, explain PIN option
3. If they want more control: explain SSO upgrade path
4. **Never** dump technical config as the first response
