# BQ-TRUST-CHANNEL-QUIET-WHEN-ENDPOINT-ABSENT-S726

**Pillar:** AIM-Channel (vectorAIz shared codebase)
**Type:** operational / customer-facing bugfix
**Session:** 726

## Problem
The vectorAIz backend (`vectoraiz-backend`, Railway production) emitted repeated
"Deploy Crashed" alerts. Root cause: the Trust Channel background client
(`app/services/trust_channel_client.py`) opens a WebSocket to
`${VECTORAIZ_AI_MARKET_URL}/ws/trust-channel`. In production that route does
**not exist** on ai.market — `GET https://api.ai.market/ws/trust-channel` → 404,
and the WS upgrade is rejected (403). The server-side Trust Channel route is not
deployed (only the verifiable-credential / JSON-LD context scaffolding exists in
ai-market-backend; no `@app.websocket("/ws/trust-channel")` handler).

Two defects amplified this into log noise / crash-alert spam:
1. **Dead except branch.** Code caught `InvalidStatusCode` (websockets <14).
   The pinned runtime is `websockets>=14,<15`, which raises `InvalidStatus`
   (status under `.response.status_code`). The rejection therefore fell through
   to the generic `except Exception ... exc_info=True`, emitting a full
   traceback every ~2s.
2. **Misleading guidance.** The dead branch's message blamed
   `VECTORAIZ_INTERNAL_API_KEY` (an auth/key problem) when the real cause is an
   absent server endpoint.

The task is `asyncio.create_task(_safe_background_task(...))` — fire-and-forget;
it never blocked startup or crashed the app. Current production deploy is
SUCCESS/serving (vectoraiz.com 200, backend routing). No customer impact; the
fix removes alert noise and connects the listener cleanly if/when the server
route ships.

## Fix (vectorAIz client only — no ai.market change, no credential change)
- Add `_ws_reject_status(exc)` helper that reads the handshake-rejection HTTP
  status across websockets 14 (`InvalidStatus.response.status_code`) and
  <14 (`InvalidStatusCode.status_code`). Version-agnostic.
- Collapse the rejection handling into one branch:
  - **404/401/403** → endpoint absent / peer not accepting in this environment.
    Treat as benign: log **once** at WARNING (no traceback), pin backoff to
    `_MAX_BACKOFF_S`, keep retrying quietly.
  - other status → log at ERROR (no traceback), normal exponential backoff.
  - no status (true unexpected error) → ERROR with `exc_info=True` (unchanged).
- `_endpoint_absent_logged` latch in `__init__`, reset on successful connect so
  a later genuine outage is reported again.

## Verification
- `python -m py_compile` + AST parse OK.
- `_ws_reject_status` resolves status on installed websockets 15.0.1 and the
  pinned 14.x surface.
- Only one `exc_info=True` in the connection loop (truly-unexpected path).

## Follow-ups (filed separately, not in this PR)
- requirements pin drift: local venv has websockets 15.0.1 vs pinned `<15`.
- ai.market: build the server-side `/ws/trust-channel` route, or formally
  retire the vectorAIz-side listener if the feature is shelved.
