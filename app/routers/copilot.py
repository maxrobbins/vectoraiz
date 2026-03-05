"""
Co-Pilot WebSocket Bridge + REST Endpoints (Metered)
=====================================================

WebSocket endpoint /ws/copilot for real-time SCI command relay between
allAI and the vectoraiz frontend. REST companion endpoints for command
injection and status checking.

**Metering Integration (BQ-073):**
- Pre-flight balance gate on every BRAIN_MESSAGE: checks cached balance
  before allowing Claude API call. Returns BALANCE_GATE if insufficient.
- Post-flight usage reporting: after each response, reports actual token
  usage to ai-market-backend for atomic credit deduction.
- Mid-stream depletion: current response completes (balance may go
  slightly negative), next request is blocked at the balance gate.
- Balance info sent on WS connect and after each metered interaction.

**BQ-128 Phase 1 — Streaming:**
- BRAIN_STREAM_CHUNK: server → client streaming token
- BRAIN_STREAM_END: server → client stream complete with usage
- REST endpoints for session history (user_id scoped)
- Allie availability check in CONNECTED message

CREATED: S94/BQ-069 (2026-02-06)
UPDATED: S94/BQ-073 (2026-02-06) — Added metering integration
UPDATED: S120/BQ-116 (2026-02-12) — WebSocket reliability: safe_send, task-based BRAIN_MESSAGE, STOP cancellation, ping nonce
UPDATED: BQ-128 Phase 1 (2026-02-14) — Streaming protocol, REST session endpoints, Allie availability
UPDATED: BQ-128 Phase 4 (2026-02-14) — WS hardening: size limits, rate limiting, nonce fix, cost-accounting
UPDATED: BQ-ALLAI-FILES (2026-02-16) — File upload endpoint, attachment handling in BRAIN_MESSAGE
SPEC: BQ-CP-01 sections 3.3, 3.4
"""

import asyncio
import json as json_mod
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, ValidationError
from sqlmodel import Session as DBSession, select

from app.core.errors import VectorAIzError
from app.config import settings
from app.core.local_only_guard import is_local_only
from app.auth.api_key_auth import AuthenticatedUser, get_current_user, get_current_user_ws
from app.models.copilot import SCICommand, CommandResult, StateSnapshot
from app.models.state import (
    Message, MessageKind, MessageRead, MessageRole,
    Session as ChatSession, SessionRead,
)
from app.services.metering_service import metering_service
from app.services.copilot_service import copilot_service
from app.services.serial_metering import (
    metered, MeterDecision, MeteringStrategy, CreditExhaustedException, ActivationRequiredException, UnprovisionedException,
    SerialMeteringStrategy, LedgerMeteringStrategy,
    _make_request_id, DEFAULT_DATA_COST, DEFAULT_SETUP_COST,
    classify_copilot_category,
)
from app.services.serial_store import get_serial_store, MIGRATED
from app.services.allie_provider import AllieDisabledError
from app.services.nudge_manager import nudge_manager, NudgeMessage
from app.services.approval_token_service import approval_token_service
from app.services.audit_logger import audit_logger
from app.services.event_bus import VZEvent, event_bus
from app.services.chat_attachment_service import (
    ALLOWED_MIME_TYPES,
    MAX_ATTACHMENTS_PER_MESSAGE,
    MAX_FILE_SIZE,
    MAX_POST_RESIZE_BYTES,
    ChatAttachment,
    chat_attachment_service,
    resize_if_needed,
    sanitize_filename,
    validate_image,
)
from app.services.mime_detector import detect_mime_for_zip, detect_mime_from_header
from app.core.database import get_legacy_session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# BQ-128 Phase 4: WebSocket Hardening Constants
# ---------------------------------------------------------------------------
MAX_BRAIN_MESSAGE_CHARS = 8000       # Max user message length
MAX_WS_PAYLOAD_BYTES = 65536         # 64KB max raw WS frame
MAX_STATE_SNAPSHOT_BYTES = 16384     # 16KB max STATE_SNAPSHOT
MAX_MESSAGES_PER_MINUTE = 30         # Per-session message rate limit
MAX_CONNECTIONS_PER_MINUTE = 10      # Per-user connection rate limit

# BQ-ALLAI-FILES: Upload rate limiting
MAX_UPLOADS_PER_MINUTE = 5
MAX_UPLOADS_PER_SESSION = 20
_upload_rate: dict[str, list[float]] = defaultdict(list)  # user_id → [timestamps]
_upload_session_count: dict[str, int] = defaultdict(int)  # user_id → total uploads


# ---------------------------------------------------------------------------
# WebSocketSendError + safe_send_json (BQ-116)
# ---------------------------------------------------------------------------

class WebSocketSendError(Exception):
    """Raised when a WebSocket send fails or times out."""


async def safe_send_json(ws: WebSocket, data: dict, timeout: float = 10.0) -> None:
    """Send JSON over WebSocket with timeout. Closes socket on failure."""
    try:
        await asyncio.wait_for(ws.send_json(data), timeout)
    except (asyncio.TimeoutError, Exception) as exc:
        try:
            await ws.close(code=1011)
        except Exception:
            pass
        raise WebSocketSendError(f"send failed: {exc}") from exc


# ---------------------------------------------------------------------------
# ConnectionManager — process-local singleton
# ---------------------------------------------------------------------------
# IMPORTANT: ConnectionManager is process-local (in-memory dict).
# This REQUIRES uvicorn to run with workers=1 (current default).
# If scaling to multiple workers, migrate to Redis PubSub relay.
# See: ADR-003 §Phase 2 scaling notes.


class ConnectionManager:
    """Manages active Co-Pilot WebSocket connections. Singleton, process-local."""

    def __init__(self) -> None:
        self._active: dict[str, WebSocket] = {}          # session_id → websocket
        self._user_sessions: dict[str, str] = {}          # user_id → session_id
        self._connected_since: dict[str, str] = {}        # session_id → ISO timestamp
        self._session_users: dict[str, AuthenticatedUser] = {}  # session_id → user
        self._session_balance: dict[str, int] = {}        # session_id → cached balance_cents
        self._inflight_task: dict[str, asyncio.Task] = {}  # session_id → BRAIN_MESSAGE task
        # BQ-128 Phase 2: STATE_SNAPSHOT cache per session
        self._session_state: dict[str, StateSnapshot] = {}  # session_id → last snapshot
        # BQ-128 Phase 2: has_seen_intro flag per session
        self._session_intro_seen: dict[str, bool] = {}   # session_id → has_seen_intro
        # Audit: asyncio.Lock for concurrent access safety (lazily created)
        self._lock: Optional[asyncio.Lock] = None
        # BQ-128 Phase 4: Rate limiting
        self._session_msg_timestamps: dict[str, list[float]] = defaultdict(list)  # session_id → [timestamps]
        self._user_connect_timestamps: dict[str, list[float]] = defaultdict(list)  # user_id → [timestamps]

    @property
    def _async_lock(self) -> asyncio.Lock:
        """Lazily create lock in the current event loop."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def connect(
        self, session_id: str, user_id: str, ws: WebSocket,
        user: Optional[AuthenticatedUser] = None,
    ) -> None:
        """Register a new WebSocket connection for a user."""
        async with self._async_lock:
            # If user already has a session, disconnect the old one
            old_session = self._user_sessions.get(user_id)
            if old_session and old_session in self._active:
                # Cancel any inflight task for the old session before replacing
                await self._cancel_inflight_unlocked(old_session)
                old_ws = self._active[old_session]
                try:
                    await old_ws.close(code=4002, reason="Replaced by new connection")
                except Exception:
                    pass  # Old connection may already be dead
                self._disconnect_unlocked(old_session)

            self._active[session_id] = ws
            self._user_sessions[user_id] = session_id
            self._connected_since[session_id] = datetime.now(timezone.utc).isoformat()

            if user is not None:
                self._session_users[session_id] = user
                # Cache initial balance from auth validation
                total_balance = (user.balance_cents or 0) + (user.free_trial_remaining_cents or 0)
                self._session_balance[session_id] = total_balance

    async def disconnect(self, session_id: str) -> None:
        """Remove a WebSocket connection (acquires lock)."""
        async with self._async_lock:
            self._disconnect_unlocked(session_id)

    def _disconnect_unlocked(self, session_id: str) -> None:
        """Remove a WebSocket connection (caller must hold _lock)."""
        self._active.pop(session_id, None)
        self._connected_since.pop(session_id, None)
        self._session_users.pop(session_id, None)
        self._session_balance.pop(session_id, None)
        self._inflight_task.pop(session_id, None)
        self._session_state.pop(session_id, None)
        self._session_intro_seen.pop(session_id, None)
        self._session_msg_timestamps.pop(session_id, None)
        # Clean up user mapping
        self._user_sessions = {
            k: v for k, v in self._user_sessions.items() if v != session_id
        }

    async def cancel_inflight(self, session_id: str) -> None:
        """Cancel any inflight task for a session."""
        await self._cancel_inflight_unlocked(session_id)

    async def _cancel_inflight_unlocked(self, session_id: str) -> None:
        """Cancel any inflight task for a session."""
        inflight = self._inflight_task.get(session_id)
        if inflight and not inflight.done():
            inflight.cancel()
            try:
                await inflight
            except asyncio.CancelledError:
                pass
        self._inflight_task.pop(session_id, None)

    async def set_inflight(self, session_id: str, task: asyncio.Task) -> None:
        """Register an inflight task for a session."""
        self._inflight_task[session_id] = task

    async def get_inflight(self, session_id: str) -> Optional[asyncio.Task]:
        """Get the inflight task for a session."""
        return self._inflight_task.get(session_id)

    async def clear_inflight(self, session_id: str) -> None:
        """Remove the inflight task reference for a session."""
        self._inflight_task.pop(session_id, None)

    def get_ws(self, session_id: str) -> Optional[WebSocket]:
        """Get the WebSocket for a session."""
        return self._active.get(session_id)

    def get_session_for_user(self, user_id: str) -> Optional[str]:
        """Get the active session ID for a user."""
        return self._user_sessions.get(user_id)

    def get_connected_since(self, session_id: str) -> Optional[str]:
        """Get the connection timestamp for a session."""
        return self._connected_since.get(session_id)

    def get_user(self, session_id: str) -> Optional[AuthenticatedUser]:
        """Get the AuthenticatedUser for a session."""
        return self._session_users.get(session_id)

    def get_balance(self, session_id: str) -> int:
        """Get the cached balance_cents for a session."""
        return self._session_balance.get(session_id, 0)

    def update_balance(self, session_id: str, new_balance: int) -> None:
        """Update the cached balance_cents for a session after usage report."""
        self._session_balance[session_id] = new_balance

    def get_state_snapshot(self, session_id: str) -> Optional[StateSnapshot]:
        """Get the cached StateSnapshot for a session."""
        return self._session_state.get(session_id)

    def set_state_snapshot(self, session_id: str, snapshot: StateSnapshot) -> None:
        """Cache a StateSnapshot for a session."""
        self._session_state[session_id] = snapshot

    def get_intro_seen(self, session_id: str) -> bool:
        """Check if the intro has been shown for this session."""
        return self._session_intro_seen.get(session_id, False)

    def set_intro_seen(self, session_id: str, seen: bool = True) -> None:
        """Mark intro as shown for this session."""
        self._session_intro_seen[session_id] = seen

    def check_message_rate(self, session_id: str) -> tuple[bool, float]:
        """Check per-session message rate limit. Returns (allowed, reset_after_seconds)."""
        now = time.monotonic()
        window = 60.0
        timestamps = self._session_msg_timestamps[session_id]
        # Prune timestamps outside the window
        self._session_msg_timestamps[session_id] = [
            t for t in timestamps if now - t < window
        ]
        timestamps = self._session_msg_timestamps[session_id]
        if len(timestamps) >= MAX_MESSAGES_PER_MINUTE:
            oldest = timestamps[0]
            reset_after = window - (now - oldest)
            return False, max(0.0, reset_after)
        timestamps.append(now)
        return True, 0.0

    def check_connection_rate(self, user_id: str) -> bool:
        """Check per-user connection rate limit. Returns True if allowed."""
        now = time.monotonic()
        window = 60.0
        timestamps = self._user_connect_timestamps[user_id]
        self._user_connect_timestamps[user_id] = [
            t for t in timestamps if now - t < window
        ]
        timestamps = self._user_connect_timestamps[user_id]
        if len(timestamps) >= MAX_CONNECTIONS_PER_MINUTE:
            return False
        timestamps.append(now)
        return True

    @property
    def has_active(self) -> bool:
        """Whether any connections are currently active."""
        return len(self._active) > 0

    @property
    def active_count(self) -> int:
        """Number of active connections."""
        return len(self._active)


# Module-level singleton
manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
# Two routers:
# - `router` — REST endpoints, mounted with prefix="/api/copilot" in main.py
# - `ws_router` — WebSocket endpoint, mounted WITHOUT prefix (path is /ws/copilot)

router = APIRouter()
ws_router = APIRouter()

# Ping interval (seconds)
PING_INTERVAL = 30
PONG_TIMEOUT = 10


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class CommandQueuedResponse(BaseModel):
    queued: bool
    command_id: str


class CoPilotStatusResponse(BaseModel):
    active: bool
    session_id: Optional[str] = None
    connected_since: Optional[str] = None


# ---------------------------------------------------------------------------
# Helper: build balance info message
# ---------------------------------------------------------------------------

def _get_or_create_user_preferences(user_id: str):
    """
    Load per-user preferences from legacy DB, creating a row on first access.
    Returns the UserPreferences ORM instance, or None on error.
    """
    from app.core.database import get_legacy_session_context
    from app.models.state import UserPreferences as UP

    try:
        with get_legacy_session_context() as db:
            stmt = select(UP).where(UP.user_id == user_id)
            prefs = db.exec(stmt).first()
            if prefs is None:
                prefs = UP(user_id=user_id)
                db.add(prefs)
                db.commit()
                db.refresh(prefs)
            return prefs
    except Exception as e:
        logger.debug("Failed to load user preferences: %s", e)
    return None


def _get_user_preferences_dict(user_id: str) -> dict:
    """
    Load user preferences from legacy DB as a dict for context injection.
    Returns empty dict if not found or on error.
    """
    prefs = _get_or_create_user_preferences(user_id)
    if prefs:
        return {
            "tone_mode": prefs.tone_mode,
            "quiet_mode": prefs.quiet_mode,
            "has_seen_intro": prefs.has_seen_intro,
        }
    return {}


def _persist_intro_seen(user_id: str) -> None:
    """Mark has_seen_intro=True in DB for the given user."""
    from app.core.database import get_legacy_session_context
    from app.models.state import UserPreferences as UP

    try:
        with get_legacy_session_context() as db:
            stmt = select(UP).where(UP.user_id == user_id)
            prefs = db.exec(stmt).first()
            if prefs and not prefs.has_seen_intro:
                prefs.has_seen_intro = True
                db.add(prefs)
                db.commit()
    except Exception as e:
        logger.debug("Failed to persist has_seen_intro: %s", e)


def _persist_nudge_dismissal(user_id: str, trigger_type: str) -> None:
    """Persist a permanent nudge dismissal to the DB."""
    from app.core.database import get_legacy_session_context
    from app.models.state import NudgeDismissal
    from sqlalchemy.exc import IntegrityError

    try:
        with get_legacy_session_context() as db:
            dismissal = NudgeDismissal(
                user_id=user_id,
                trigger_type=trigger_type,
                permanent=True,
            )
            db.add(dismissal)
            db.commit()
    except IntegrityError:
        db.rollback()  # Required to reset session state after IntegrityError
        logger.debug("Nudge dismissal already exists: user=%s trigger=%s", user_id, trigger_type)
    except Exception as e:
        logger.warning("Failed to persist nudge dismissal: user=%s trigger=%s: %s", user_id, trigger_type, e)


def _load_nudge_dismissals(user_id: str) -> List[str]:
    """Load permanent nudge dismissals from DB for a user."""
    from app.core.database import get_legacy_session_context
    from app.models.state import NudgeDismissal

    try:
        with get_legacy_session_context() as db:
            stmt = select(NudgeDismissal).where(
                NudgeDismissal.user_id == user_id,
                NudgeDismissal.permanent == True,  # noqa: E712
            )
            dismissals = db.exec(stmt).all()
            return [d.trigger_type for d in dismissals]
    except Exception as e:
        logger.debug("Failed to load nudge dismissals: user=%s: %s", user_id, e)
    return []


async def send_nudge_to_session(
    session_id: str, trigger: str, context: dict, user_id: str = "",
) -> Optional[NudgeMessage]:
    """
    Attempt to send a nudge to a connected session. Called by backend event handlers.

    Returns the NudgeMessage if sent, None if suppressed or session not found.
    """
    if not user_id:
        logger.error("send_nudge_to_session called without user_id — skipping nudge")
        return None

    ws = manager.get_ws(session_id)
    if not ws:
        return None

    nudge = await nudge_manager.maybe_nudge(
        trigger=trigger,
        context=context,
        session_id=session_id,
        user_id=user_id,
    )
    if nudge is None:
        return None

    try:
        await safe_send_json(ws, nudge_manager.to_ws_message(nudge))

        # Persist nudge as a message with kind=nudge
        try:
            from app.core.database import get_legacy_session_context
            with get_legacy_session_context() as db:
                chat_session = _get_or_create_chat_session(user_id, db) if user_id else None
                if chat_session:
                    _persist_message(
                        db, chat_session,
                        role=MessageRole.SYSTEM,
                        content=nudge.message,
                        kind=MessageKind.NUDGE,
                    )
        except Exception as pe:
            logger.debug("Failed to persist nudge message: %s", pe)

        return nudge
    except WebSocketSendError:
        logger.debug("Failed to send nudge: session=%s trigger=%s", session_id, trigger)
        return None


def _balance_info_message(balance_cents: int, reason: Optional[str] = None) -> dict:
    """Build a BALANCE_INFO WebSocket message."""
    msg: dict = {
        "type": "BALANCE_INFO",
        "balance_cents": balance_cents,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if reason:
        msg["reason"] = reason
    return msg


def _balance_gate_message(balance_cents: int, reason: str) -> dict:
    """Build a BALANCE_GATE WebSocket message (402 equivalent for WS)."""
    return {
        "type": "BALANCE_GATE",
        "balance_cents": balance_cents,
        "message": "Purchase credits to use Co-Pilot",
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Session persistence helpers (legacy DB)
# ---------------------------------------------------------------------------

def _get_or_create_chat_session(
    user_id: str, db: DBSession, session_id_hint: Optional[str] = None,
) -> ChatSession:
    """Get the user's current (non-archived) chat session, or create one."""
    stmt = (
        select(ChatSession)
        .where(ChatSession.user_id == user_id, ChatSession.archived == False)  # noqa: E712
        .order_by(ChatSession.created_at.desc())
    )
    session = db.exec(stmt).first()
    if session:
        return session

    session = ChatSession(user_id=user_id, title="Allie Chat")
    db.add(session)
    db.commit()
    db.refresh(session)
    return session


def _persist_message(
    db: DBSession,
    session: ChatSession,
    role: MessageRole,
    content: str,
    kind: str = MessageKind.CHAT,
    client_message_id: Optional[str] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    cost_cents: Optional[int] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> Message:
    """Persist a message to the legacy DB. Returns the created Message."""
    msg = Message(
        session_id=session.id,
        role=role,
        content=content,
        kind=kind,
        client_message_id=client_message_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_cents=cost_cents,
        provider=provider,
        model=model,
    )
    db.add(msg)
    session.total_message_count = (session.total_message_count or 0) + 1
    db.add(session)
    db.commit()
    db.refresh(msg)
    return msg


def _persist_messages_atomic(
    db: DBSession,
    session: ChatSession,
    user_message: str,
    assistant_text: str,
    client_message_id: Optional[str] = None,
    usage=None,
) -> None:
    """Persist user + assistant message in a single transaction."""
    user_msg = Message(
        session_id=session.id,
        role=MessageRole.USER,
        content=user_message,
        kind=MessageKind.CHAT,
        client_message_id=client_message_id,
    )
    assistant_msg = Message(
        session_id=session.id,
        role=MessageRole.ASSISTANT,
        content=assistant_text,
        kind=MessageKind.CHAT,
        input_tokens=usage.input_tokens if usage else None,
        output_tokens=usage.output_tokens if usage else None,
        cost_cents=usage.cost_cents if usage else None,
        provider=usage.provider if usage else None,
        model=usage.model if usage else None,
    )
    db.add(user_msg)
    db.add(assistant_msg)
    session.total_message_count = (session.total_message_count or 0) + 2
    db.add(session)
    db.commit()


# ---------------------------------------------------------------------------
# WebSocket endpoint: /ws/copilot
# ---------------------------------------------------------------------------

@ws_router.websocket("/ws/copilot")
async def websocket_copilot(websocket: WebSocket):
    """
    Co-Pilot WebSocket endpoint.

    Connection flow:
    1. Client connects with ?token=aim_xxx
    2. Server validates via get_current_user_ws
    3. If valid: sends CONNECTED message with session_id, balance, and allie info
    4. Bidirectional message loop (PING/PONG, commands, state snapshots, brain messages)
    5. Server sends PING every 30s; client must PONG within 10s

    BQ-128: BRAIN_MESSAGE now streams via BRAIN_STREAM_CHUNK/BRAIN_STREAM_END.
    CONNECTED message includes allie_available and is_standalone flags.

    Close codes:
    - 4001: Unauthorized (missing/invalid token)
    - 4002: Replaced by new connection
    - 1000: Normal closure
    - 1011: Unexpected condition (send failure)
    """
    # --- Authentication ---
    user = await get_current_user_ws(websocket)
    if user is None:
        await websocket.accept()
        await websocket.close(code=4001, reason="Unauthorized")
        return

    # BQ-128 Phase 4: Per-user connection rate limiting
    if not manager.check_connection_rate(user.user_id):
        await websocket.accept()
        await websocket.close(code=4029, reason="Too many connections")
        logger.warning("Connection rate limit exceeded: user=%s", user.user_id)
        return

    await websocket.accept()

    # --- Generate session ID and register ---
    session_id = f"cps_{uuid.uuid4().hex[:16]}"
    await manager.connect(session_id, user.user_id, websocket, user=user)

    logger.info(
        f"Co-Pilot WS connected: session={session_id} user={user.user_id}"
    )

    # BQ-128 Phase 3: Load permanent nudge dismissals for this user
    dismissed_triggers = _load_nudge_dismissals(user.user_id)
    if dismissed_triggers:
        nudge_manager.load_permanent_dismissals(user.user_id, dismissed_triggers)

    # Load quiet mode from user preferences
    user_prefs = _get_user_preferences_dict(user.user_id)
    if user_prefs.get("quiet_mode", False):
        nudge_manager.set_quiet_mode(session_id, True)

    # BQ-128: Allie availability check
    allie_available = settings.allai_enabled and not is_local_only()
    standalone = is_local_only()

    # Send CONNECTED message with balance info + allie flags (BQ-128 Task 1.6)
    total_balance = (user.balance_cents or 0) + (user.free_trial_remaining_cents or 0)
    try:
        await safe_send_json(websocket, {
            "type": "CONNECTED",
            "session_id": session_id,
            "balance_cents": total_balance,
            "allie_available": allie_available,
            "is_standalone": standalone,
            "rate_limit": {
                "remaining_tokens_today": 100000 if allie_available else 0,
                "daily_limit": 100000,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        # Balance gating: In standalone mode allAI is off (no credits apply).
        # In connected mode ai.market proxy handles billing — no local gate needed.
    except WebSocketSendError:
        logger.error(f"Failed to send CONNECTED message: session={session_id}")
        manager.disconnect(session_id)
        return

    # --- Keepalive ping task with nonce ---
    pong_received = asyncio.Event()
    pong_received.set()  # Start as set (no pending ping)
    expected_nonce: Optional[str] = None

    async def ping_loop():
        nonlocal expected_nonce
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                pong_received.clear()
                nonce = uuid.uuid4().hex[:8]
                expected_nonce = nonce
                try:
                    await safe_send_json(websocket, {"type": "PING", "nonce": nonce})
                except WebSocketSendError:
                    break

                # Wait for PONG with timeout
                try:
                    await asyncio.wait_for(pong_received.wait(), timeout=PONG_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.warning(
                        f"Co-Pilot WS PONG timeout: session={session_id}"
                    )
                    try:
                        await websocket.close(code=1001, reason="PONG timeout")
                    except Exception:
                        pass
                    break
        except asyncio.CancelledError:
            pass

    ping_task = asyncio.create_task(ping_loop())

    # --- BRAIN_MESSAGE streaming task wrapper (BQ-128) ---
    async def _brain_stream_task(
        sid: str, ws: WebSocket, session_user: AuthenticatedUser,
        user_message: str, cached_balance: int,
        message_id: Optional[str] = None,
        client_message_id: Optional[str] = None,
        attachments: Optional[List[ChatAttachment]] = None,
    ) -> None:
        """Process a BRAIN_MESSAGE with streaming. Sends BRAIN_STREAM_CHUNK + BRAIN_STREAM_END."""
        from sqlalchemy.exc import IntegrityError as SAIntegrityError

        msg_id = message_id or f"msg_{uuid.uuid4().hex[:12]}"
        usage = None  # Captured early for cost-accounting in finally block

        # BQ-128 Phase 4: Persist user message BEFORE streaming (never lose inbound text)
        try:
            from app.core.database import get_legacy_session_context
            with get_legacy_session_context() as db:
                chat_session = _get_or_create_chat_session(session_user.user_id, db)

                # App-level idempotency check: skip if client_message_id already exists
                if client_message_id:
                    dup_stmt = select(Message).where(
                        Message.session_id == chat_session.id,
                        Message.client_message_id == client_message_id,
                    )
                    if db.exec(dup_stmt).first():
                        logger.info(
                            "Duplicate client_message_id=%s in session=%s user=%s — idempotent success",
                            client_message_id, sid, session_user.user_id,
                        )
                    else:
                        try:
                            _persist_message(
                                db, chat_session,
                                role=MessageRole.USER,
                                content=user_message,
                                kind=MessageKind.CHAT,
                                client_message_id=client_message_id,
                            )
                        except SAIntegrityError:
                            db.rollback()
                            logger.info(
                                "DB idempotency constraint caught duplicate client_message_id=%s session=%s",
                                client_message_id, sid,
                            )
                else:
                    _persist_message(
                        db, chat_session,
                        role=MessageRole.USER,
                        content=user_message,
                        kind=MessageKind.CHAT,
                    )
        except Exception as pe:
            logger.warning(
                "Failed to persist user message: session=%s user=%s msg=%s: %s",
                sid, session_user.user_id, msg_id, pe,
            )

        try:
            # Send chunk callback
            async def send_chunk(text: str) -> None:
                await safe_send_json(ws, {
                    "type": "BRAIN_STREAM_CHUNK",
                    "chunk": text,
                    "message_id": msg_id,
                })

            # BQ-ALLAI-B: Send arbitrary WS JSON (for TOOL_STATUS, TOOL_RESULT, etc.)
            async def send_ws_json(data: dict) -> None:
                await safe_send_json(ws, data)

            # BQ-ALLAI-B: Heartbeat to keep WS alive during tool execution
            async def send_heartbeat() -> None:
                try:
                    await safe_send_json(ws, {"type": "HEARTBEAT"}, timeout=5.0)
                except WebSocketSendError:
                    pass  # Best-effort heartbeat

            # BQ-128 Phase 2: Pass context + preferences to pipeline
            state_snapshot = manager.get_state_snapshot(sid)
            user_prefs = _get_user_preferences_dict(session_user.user_id)

            # Load conversation history (last 20 messages = ~10 turns)
            chat_history: list[dict[str, str]] = []
            try:
                from app.core.database import get_legacy_session_context
                with get_legacy_session_context() as hist_db:
                    chat_session = _get_or_create_chat_session(session_user.user_id, hist_db)
                    hist_stmt = (
                        select(Message)
                        .where(Message.session_id == chat_session.id)
                        .where(Message.kind == MessageKind.CHAT)
                        .order_by(Message.created_at.asc())
                    )
                    all_msgs = hist_db.exec(hist_stmt).all()
                    # Take last 20 messages, but exclude the very last one if it's
                    # the user message we just persisted (it gets added fresh via user_content)
                    recent = all_msgs[-21:]  # grab 21 so we can drop the trailing user msg
                    for m in recent:
                        chat_history.append({"role": m.role, "content": m.content})
                    # Drop the trailing user message (just persisted above)
                    if chat_history and chat_history[-1]["role"] == MessageRole.USER:
                        chat_history.pop()
                    # Cap at 20
                    chat_history = chat_history[-20:]
            except Exception as hist_err:
                logger.warning("Failed to load chat history: session=%s err=%s", sid, hist_err)

            # Intro behavior: check DB first, then in-memory cache
            if not manager.get_intro_seen(sid):
                # Load from DB on first check (survives restarts)
                if user_prefs.get("has_seen_intro", False):
                    manager.set_intro_seen(sid, True)
            is_first = not manager.get_intro_seen(sid)

            # BQ-ALLAI-B: Use agentic loop (tools via ai.market proxy) in connected mode
            if not is_local_only():
                try:
                    full_text, usage = await copilot_service.process_message_agentic(
                        user=session_user,
                        message=user_message,
                        session_id=sid,
                        message_id=msg_id,
                        send_chunk=send_chunk,
                        send_ws=send_ws_json,
                        send_heartbeat=send_heartbeat,
                        state_snapshot=state_snapshot,
                        user_preferences=user_prefs,
                        is_first_message=is_first,
                        attachments=attachments,
                        chat_history=chat_history,
                    )
                except (AllieDisabledError, asyncio.CancelledError):
                    raise
                except Exception as agentic_err:
                    logger.warning(
                        "Agentic loop failed, falling back to streaming: session=%s err=%s",
                        sid, agentic_err,
                    )
                    full_text, usage = await copilot_service.process_message_streaming(
                        user=session_user,
                        message=user_message,
                        session_id=sid,
                        message_id=msg_id,
                        send_chunk=send_chunk,
                        state_snapshot=state_snapshot,
                        user_preferences=user_prefs,
                        is_first_message=is_first,
                        attachments=attachments,
                        chat_history=chat_history,
                    )
            else:
                # Standalone mode — no tools, use classic streaming path
                full_text, usage = await copilot_service.process_message_streaming(
                    user=session_user,
                    message=user_message,
                    session_id=sid,
                    message_id=msg_id,
                    send_chunk=send_chunk,
                    state_snapshot=state_snapshot,
                    user_preferences=user_prefs,
                    is_first_message=is_first,
                    attachments=attachments,
                    chat_history=chat_history,
                )

            # Mark intro as seen after first successful response (memory + DB)
            if is_first:
                manager.set_intro_seen(sid, True)
                _persist_intro_seen(session_user.user_id)

            # Build usage dict for BRAIN_STREAM_END
            usage_dict = {}
            if usage:
                usage_dict = {
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cost_cents": usage.cost_cents,
                    "provider": usage.provider,
                    "model": usage.model,
                }

            # Send BRAIN_STREAM_END
            await safe_send_json(ws, {
                "type": "BRAIN_STREAM_END",
                "message_id": msg_id,
                "full_text": full_text,
                "usage": usage_dict,
            })

            # Persist assistant response
            try:
                from app.core.database import get_legacy_session_context
                with get_legacy_session_context() as db:
                    chat_session = _get_or_create_chat_session(session_user.user_id, db)
                    _persist_message(
                        db, chat_session,
                        role=MessageRole.ASSISTANT,
                        content=full_text,
                        kind=MessageKind.CHAT,
                        input_tokens=usage.input_tokens if usage else None,
                        output_tokens=usage.output_tokens if usage else None,
                        cost_cents=usage.cost_cents if usage else None,
                        provider=usage.provider if usage else None,
                        model=usage.model if usage else None,
                    )
            except Exception as pe:
                logger.warning(
                    "Failed to persist assistant message: session=%s user=%s msg=%s: %s",
                    sid, session_user.user_id, msg_id, pe,
                )

        except asyncio.CancelledError:
            # STOP was requested — send STOPPED (best-effort)
            try:
                await safe_send_json(ws, {
                    "type": "STOPPED",
                    "message_id": msg_id,
                    "message": "Request cancelled by user",
                })
            except WebSocketSendError:
                pass
            raise  # re-raise so task shows as cancelled

        except AllieDisabledError as e:
            try:
                await safe_send_json(ws, {
                    "type": "ERROR",
                    "message": str(e),
                    "code": "ALLIE_DISABLED",
                })
            except WebSocketSendError:
                pass

        except WebSocketSendError:
            logger.error(f"BRAIN_MESSAGE send failed: session={sid}")

        except HTTPException as he:
            if he.status_code == 402:
                try:
                    await safe_send_json(ws, _balance_gate_message(cached_balance, "insufficient_balance"))
                except WebSocketSendError:
                    pass
            else:
                try:
                    await safe_send_json(ws, {"type": "ERROR", "message": f"Co-Pilot error: {he.detail}"})
                except WebSocketSendError:
                    pass

        except Exception as e:
            logger.error(f"BRAIN_MESSAGE error: session={sid} error={e}", exc_info=True)
            try:
                await safe_send_json(ws, {"type": "ERROR", "message": "An error occurred processing your message"})
            except WebSocketSendError:
                pass

        finally:
            # BQ-128 Phase 4: Report usage in finally block — fires even on cancellation
            if usage and not is_local_only():
                try:
                    report = await metering_service.report_usage(
                        user_id=session_user.user_id,
                        service="copilot",
                        model=usage.model,
                        input_tokens=usage.input_tokens,
                        output_tokens=usage.output_tokens,
                        session_id=sid,
                        message_id=msg_id,
                    )
                    if report is not None:
                        manager.update_balance(sid, report.new_balance_cents)
                        if not report.allowed:
                            try:
                                await safe_send_json(ws, _balance_gate_message(
                                    report.new_balance_cents, "balance_depleted"
                                ))
                            except WebSocketSendError:
                                pass
                except Exception as ue:
                    logger.warning(
                        "Failed to report usage after cancel/error: "
                        "session=%s user=%s msg=%s usage=%s err=%s",
                        sid, session_user.user_id, msg_id,
                        {"input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
                         "cost_cents": usage.cost_cents, "model": usage.model},
                        ue,
                    )
            await manager.clear_inflight(sid)

    # --- Message loop ---
    try:
        while True:
            # BQ-128 Phase 4: Receive raw text to validate payload size before parsing
            raw_text = await websocket.receive_text()

            # Payload size check
            if len(raw_text.encode("utf-8")) > MAX_WS_PAYLOAD_BYTES:
                logger.warning(
                    "Oversized WS payload: session=%s bytes=%d limit=%d",
                    session_id, len(raw_text.encode("utf-8")), MAX_WS_PAYLOAD_BYTES,
                )
                try:
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": f"Payload too large (max {MAX_WS_PAYLOAD_BYTES} bytes)",
                        "code": "PAYLOAD_TOO_LARGE",
                    })
                except WebSocketSendError:
                    pass
                continue

            try:
                data = json_mod.loads(raw_text)
            except (json_mod.JSONDecodeError, ValueError):
                await safe_send_json(websocket, {
                    "type": "ERROR",
                    "message": "Invalid JSON payload",
                })
                continue

            msg_type = data.get("type", "")

            if msg_type == "PONG":
                client_nonce = data.get("nonce")
                if client_nonce is None:
                    # Missing nonce — tolerate for backwards compat
                    logger.debug(
                        "PONG missing nonce: session=%s — accepting for backwards compat",
                        session_id,
                    )
                    pong_received.set()
                elif expected_nonce and client_nonce != expected_nonce:
                    # Wrong nonce — potential hijack, do NOT accept
                    logger.warning(
                        "PONG nonce mismatch (expected=%s, got=%s): session=%s — rejecting",
                        expected_nonce, client_nonce, session_id,
                    )
                else:
                    # Correct nonce
                    pong_received.set()

            elif msg_type == "STATE_SNAPSHOT":
                # BQ-128 Phase 4: Validate snapshot size before processing
                snapshot_bytes = len(json_mod.dumps(data).encode("utf-8"))
                if snapshot_bytes > MAX_STATE_SNAPSHOT_BYTES:
                    logger.warning(
                        "Oversized STATE_SNAPSHOT: session=%s bytes=%d limit=%d",
                        session_id, snapshot_bytes, MAX_STATE_SNAPSHOT_BYTES,
                    )
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": f"STATE_SNAPSHOT too large (max {MAX_STATE_SNAPSHOT_BYTES} bytes)",
                        "code": "SNAPSHOT_TOO_LARGE",
                    })
                    continue

                # Frontend reporting current UI state — cache for context injection
                try:
                    snapshot_data = {k: v for k, v in data.items() if k != "type"}
                    snapshot = StateSnapshot(**snapshot_data)
                    manager.set_state_snapshot(session_id, snapshot)
                    logger.debug(
                        f"State snapshot from {session_id}: "
                        f"route={snapshot.current_route}"
                    )
                except ValidationError as e:
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": f"Invalid STATE_SNAPSHOT: {e.error_count()} errors",
                    })

            elif msg_type == "COMMAND_RESULT":
                # Frontend reporting result of executed command
                try:
                    result = CommandResult(**data)
                    logger.info(
                        f"Command result from {session_id}: "
                        f"cmd={result.command_id} success={result.success}"
                    )
                    # Phase 2: Forward result to allAI brain
                except ValidationError as e:
                    logger.warning(
                        f"Invalid COMMAND_RESULT from {session_id}: {e}"
                    )

            elif msg_type == "NAVIGATION_COMPLETE":
                # Frontend confirms route change settled
                logger.debug(
                    f"Navigation complete from {session_id}: "
                    f"route={data.get('route', 'unknown')}"
                )

            elif msg_type == "APPROVAL":
                # User approved/rejected a command
                command_id = data.get("command_id")
                approved = data.get("approved", False)
                logger.info(
                    f"Approval from {session_id}: "
                    f"cmd={command_id} approved={approved}"
                )
                # Phase 2: BQ-CP-05 Approval Engine

            elif msg_type == "STOP":
                # Cancel inflight BRAIN_MESSAGE task if running
                stop_message_id = data.get("message_id")
                logger.info(f"STOP received from {session_id} message_id={stop_message_id}")
                inflight = await manager.get_inflight(session_id)
                if inflight and not inflight.done():
                    inflight.cancel()
                    try:
                        await inflight
                    except asyncio.CancelledError:
                        pass
                    # STOPPED message is sent by _brain_stream_task CancelledError handler
                else:
                    stopped_msg: dict = {
                        "type": "STOPPED",
                        "message": "Request cancelled by user",
                    }
                    if stop_message_id:
                        stopped_msg["message_id"] = stop_message_id
                    await safe_send_json(websocket, stopped_msg)

            elif msg_type == "BRAIN_MESSAGE":
                # ----------------------------------------------------------
                # Metered Co-Pilot Brain interaction (BQ-073 / BQ-116 / BQ-128)
                # Now uses streaming protocol: BRAIN_STREAM_CHUNK + BRAIN_STREAM_END
                # ----------------------------------------------------------
                user_message = data.get("message", "")
                if not user_message:
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": "BRAIN_MESSAGE requires a non-empty 'message' field",
                    })
                    continue

                # BQ-128 Phase 4: Message size limit
                if len(user_message) > MAX_BRAIN_MESSAGE_CHARS:
                    logger.warning(
                        "Oversized BRAIN_MESSAGE: session=%s chars=%d limit=%d",
                        session_id, len(user_message), MAX_BRAIN_MESSAGE_CHARS,
                    )
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": f"Message too long (max {MAX_BRAIN_MESSAGE_CHARS} characters)",
                        "code": "MESSAGE_TOO_LARGE",
                    })
                    continue

                # BQ-128 Phase 4: Per-session message rate limiting
                rate_allowed, reset_after = manager.check_message_rate(session_id)
                if not rate_allowed:
                    logger.warning(
                        "Message rate limit exceeded: session=%s reset_after=%.1fs",
                        session_id, reset_after,
                    )
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": "Rate limit exceeded, please slow down",
                        "code": "RATE_LIMITED",
                        "reset_after_seconds": round(reset_after, 1),
                    })
                    continue

                # Reject if a task is already inflight for this session
                existing = await manager.get_inflight(session_id)
                if existing and not existing.done():
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": "Previous request still processing",
                    })
                    continue

                session_user = manager.get_user(session_id)
                cached_balance = manager.get_balance(session_id)

                # Pre-flight balance check
                # In connected mode, ai.market proxy handles billing — skip local check
                # In standalone mode, Allie is disabled anyway
                # Balance gate only applies if running with local LLM keys + local billing
                pass  # BQ-128: balance enforcement moved to ai.market proxy layer

                # BQ-VZ-SERIAL-CLIENT: Serial metering check before LLM call
                # Dual-category: classify based on active_view from frontend
                try:
                    store = get_serial_store()
                    _active_view = data.get("active_view")
                    _meter_category = classify_copilot_category(_active_view)
                    _meter_cost = DEFAULT_SETUP_COST if _meter_category == "setup" else DEFAULT_DATA_COST
                    if store.state.state == MIGRATED:
                        _strategy = LedgerMeteringStrategy()
                    else:
                        _strategy = SerialMeteringStrategy(store)
                    _req_id = _make_request_id(store.state.serial, "ws:brain_msg")
                    await _strategy.check_and_meter(_meter_category, _meter_cost, _req_id)
                except CreditExhaustedException as cex:
                    register_url = f"https://ai.market/register?serial={cex.serial}" if cex.serial else "https://ai.market/register"
                    await safe_send_json(websocket, {
                        "type": "CREDIT_WALL",
                        "category": cex.category,
                        "message": f"You've used your free {cex.category} credits. Add a payment method to continue.",
                        "setup_remaining_usd": cex.setup_remaining_usd,
                        "register_url": register_url,
                    })
                    continue
                except ActivationRequiredException as aex:
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": str(aex),
                        "code": "ACTIVATION_REQUIRED",
                    })
                    continue
                except UnprovisionedException as uex:
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": str(uex),
                        "code": "UNPROVISIONED",
                    })
                    continue
                except Exception as meter_exc:
                    logger.debug("Serial metering check failed (allowing): %s", meter_exc)

                if session_user is None:
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": "Session user not found",
                    })
                    continue

                # BQ-ALLAI-FILES: Resolve attachments from BRAIN_MESSAGE
                attachment_refs = data.get("attachments", [])
                resolved_attachments: list[ChatAttachment] = []

                if attachment_refs:
                    if len(attachment_refs) > MAX_ATTACHMENTS_PER_MESSAGE:
                        await safe_send_json(websocket, {
                            "type": "ERROR",
                            "message": f"Maximum {MAX_ATTACHMENTS_PER_MESSAGE} attachments per message",
                            "code": "TOO_MANY_ATTACHMENTS",
                        })
                        continue

                    att_error = False
                    for ref in attachment_refs:
                        att = chat_attachment_service.get(ref.get("id", ""))
                        if not att:
                            await safe_send_json(websocket, {
                                "type": "ERROR",
                                "message": f"Attachment {ref.get('id', '')[:16]} not found or expired",
                                "code": "ATTACHMENT_NOT_FOUND",
                            })
                            att_error = True
                            break
                        # AuthZ binding (M10): verify attachment belongs to this user
                        if att.user_id != session_user.user_id:
                            await safe_send_json(websocket, {
                                "type": "ERROR",
                                "message": "Attachment access denied",
                                "code": "ATTACHMENT_FORBIDDEN",
                            })
                            att_error = True
                            break
                        resolved_attachments.append(att)
                    if att_error:
                        continue

                # Spawn as task — receive loop stays responsive for STOP
                brain_message_id = data.get("message_id")
                client_msg_id = data.get("client_message_id")
                task = asyncio.create_task(
                    _brain_stream_task(
                        session_id, websocket, session_user, user_message, cached_balance,
                        message_id=brain_message_id,
                        client_message_id=client_msg_id,
                        attachments=resolved_attachments or None,
                    )
                )
                await manager.set_inflight(session_id, task)

            elif msg_type == "CONFIRM_ACTION":
                # BQ-VZ-CP2: User approved a mutation via ApprovalTokenService
                confirm_id = data.get("confirm_id", "")
                if not confirm_id:
                    await safe_send_json(websocket, {
                        "type": "ERROR",
                        "message": "CONFIRM_ACTION requires confirm_id",
                    })
                    continue

                approval_result = approval_token_service.validate_and_consume(
                    token_id=confirm_id,
                    user_id=user.user_id,
                    session_id=session_id,
                )

                if not approval_result.success:
                    await safe_send_json(websocket, {
                        "type": "CONFIRM_RESULT",
                        "confirm_id": confirm_id,
                        "success": False,
                        "message": f"Approval failed: {approval_result.reason}",
                    })
                    await event_bus.emit(session_id, VZEvent(
                        event_type="approval_result",
                        session_id=session_id,
                        data={"token_id": confirm_id, "success": False, "reason": approval_result.reason},
                    ))
                    continue

                # Execute via AllAIToolExecutor with full audit + redaction
                try:
                    from app.services.allai_tool_executor import AllAIToolExecutor
                    executor = AllAIToolExecutor(
                        user=session_user,
                        send_ws=lambda msg: safe_send_json(websocket, msg),
                        session_id=session_id,
                    )
                    exec_result = await executor.execute_approved(
                        tool_name=approval_result.tool_name,
                        tool_input=approval_result.tool_input,
                        approval_token_id=confirm_id,
                    )
                    await safe_send_json(websocket, {
                        "type": "CONFIRM_RESULT",
                        "confirm_id": confirm_id,
                        "success": True,
                        "message": exec_result.llm_summary[:300],
                    })
                    await event_bus.emit(session_id, VZEvent(
                        event_type="approval_result",
                        session_id=session_id,
                        data={"token_id": confirm_id, "success": True, "tool_name": approval_result.tool_name},
                    ))
                except Exception as e:
                    logger.error("CONFIRM_ACTION execution failed: %s", e)
                    await safe_send_json(websocket, {
                        "type": "CONFIRM_RESULT",
                        "confirm_id": confirm_id,
                        "success": False,
                        "message": f"Execution failed: {str(e)[:200]}",
                    })
                    await event_bus.emit(session_id, VZEvent(
                        event_type="approval_result",
                        session_id=session_id,
                        data={"token_id": confirm_id, "success": False, "reason": "execution_error"},
                    ))

            elif msg_type == "NUDGE_DISMISS":
                # BQ-128 Phase 3: User dismissed a nudge
                nudge_trigger = data.get("trigger", "")
                nudge_id = data.get("nudge_id", "")
                permanent = data.get("permanent", False)

                # Validate trigger against allowlist
                if nudge_trigger not in nudge_manager.TRIGGER_ALLOWLIST:
                    logger.info("Invalid nudge trigger in NUDGE_DISMISS: %s", nudge_trigger)
                    continue

                # For permanent dismissals, verify the nudge was actually issued to this session
                if permanent and not nudge_manager.was_nudge_issued(session_id, nudge_id):
                    logger.info(
                        "Permanent dismiss rejected — nudge not issued: nudge_id=%s session=%s",
                        nudge_id, session_id,
                    )
                    continue

                nudge_manager.record_dismissal(
                    session_id=session_id,
                    trigger=nudge_trigger,
                    permanent=permanent,
                    user_id=user.user_id,
                    nudge_id=nudge_id,
                )
                # Persist permanent dismissals to DB
                if permanent:
                    _persist_nudge_dismissal(user.user_id, nudge_trigger)
                logger.info(
                    "Nudge dismissed: trigger=%s permanent=%s session=%s",
                    nudge_trigger, permanent, session_id,
                )

            else:
                await safe_send_json(websocket, {
                    "type": "ERROR",
                    "message": f"Unknown message type: {msg_type}",
                })

    except WebSocketDisconnect:
        logger.info(f"Co-Pilot WS disconnected: session={session_id}")
    except WebSocketSendError:
        logger.warning(f"Co-Pilot WS send error, closing: session={session_id}")
    except Exception as e:
        logger.error(f"Co-Pilot WS error: session={session_id} error={e}")
    finally:
        # Cleanup order: inflight task → ping task → websocket → manager
        await manager.cancel_inflight(session_id)

        ping_task.cancel()
        try:
            await ping_task
        except asyncio.CancelledError:
            pass

        try:
            await websocket.close(code=1000)
        except Exception:
            pass

        await manager.disconnect(session_id)
        nudge_manager.cleanup_session(session_id)
        logger.info(f"Co-Pilot WS cleaned up: session={session_id}")


# ---------------------------------------------------------------------------
# SSE endpoint: GET /api/copilot/events/stream
# ---------------------------------------------------------------------------

@router.get(
    "/events/stream",
    summary="SSE event stream for approval requests and tool execution events",
)
async def events_stream(
    session_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Server-Sent Events stream for a session. Multi-tab safe."""
    import asyncio
    from starlette.responses import StreamingResponse

    queue = event_bus.subscribe(session_id)

    async def _generate():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield event.to_sse()
                except asyncio.TimeoutError:
                    # Keepalive ping
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            event_bus.unsubscribe(session_id, queue)

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# REST endpoint: POST /api/copilot/command
# ---------------------------------------------------------------------------

@router.post(
    "/command",
    response_model=CommandQueuedResponse,
    summary="Send SCI command to connected Co-Pilot session",
    description=(
        "Validates the SCI command and forwards it to the user's active "
        "WebSocket session. Returns 409 if no session is active."
    ),
)
async def send_command(
    command: SCICommand,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    REST-to-WebSocket relay for SCI commands.

    allAI (or any authenticated caller) sends an SCI command here.
    The backend looks up the user's active WebSocket session and pushes
    the command to the connected frontend.
    """
    session_id = manager.get_session_for_user(user.user_id)
    if not session_id:
        raise VectorAIzError("VAI-COP-001", detail="No active WebSocket session for user")

    ws = manager.get_ws(session_id)
    if not ws:
        # Session exists in mapping but WS is gone — clean up
        await manager.disconnect(session_id)
        raise VectorAIzError("VAI-COP-001", detail="WebSocket disconnected, session stale")

    # Forward command to frontend via WebSocket
    try:
        await safe_send_json(ws, command.model_dump())
        logger.info(
            f"Command relayed: cmd={command.id} action={command.action} "
            f"→ session={session_id}"
        )
    except WebSocketSendError:
        logger.error(f"Failed to relay command to {session_id}")
        try:
            await ws.close(code=1011)
        except Exception:
            pass
        await manager.disconnect(session_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to send command to co-pilot session.",
        )

    return CommandQueuedResponse(queued=True, command_id=command.id)


# ---------------------------------------------------------------------------
# REST endpoint: POST /api/copilot/brain
# ---------------------------------------------------------------------------

class BrainMessageRequest(BaseModel):
    """Request model for REST-based Co-Pilot brain interaction."""
    message: str
    session_id: str = ""
    message_id: Optional[str] = None
    active_view: Optional[str] = None  # BQ-VZ-SERIAL-CLIENT: dual-category classification


class BrainMessageResponse(BaseModel):
    """Response model for REST-based Co-Pilot brain interaction."""
    message: str
    balance_cents: int
    cost_cents: int


@router.post(
    "/brain",
    response_model=BrainMessageResponse,
    summary="Send a message to the Co-Pilot brain (metered)",
    description=(
        "Sends a message to the Co-Pilot brain for processing. "
        "Checks credit balance before making the LLM call. "
        "Returns 402 if balance is insufficient."
    ),
    responses={
        402: {
            "description": "Insufficient credits",
            "content": {
                "application/json": {
                    "example": {"detail": "Purchase credits to use Co-Pilot"}
                }
            },
        },
    },
)
async def brain_message(
    request: BrainMessageRequest,
    http_request: Request,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    REST endpoint for metered Co-Pilot brain interaction.

    Pre-flight balance check → LLM call → post-flight usage report.
    Returns 402 with 'Purchase credits to use Co-Pilot' if balance
    is zero or insufficient.
    """
    # BQ-VZ-SERIAL-CLIENT: dual-category serial metering
    store = get_serial_store()
    _cat = classify_copilot_category(request.active_view)
    _cost = DEFAULT_SETUP_COST if _cat == "setup" else DEFAULT_DATA_COST
    if store.state.state == MIGRATED:
        _strat: MeteringStrategy = LedgerMeteringStrategy()
    else:
        _strat = SerialMeteringStrategy(store)
    _rid = _make_request_id(store.state.serial, f"POST:/api/copilot/brain")
    await _strat.check_and_meter(_cat, _cost, _rid)

    # Pre-flight balance check
    total_balance = (user.balance_cents or 0) + (user.free_trial_remaining_cents or 0)
    balance_check = metering_service.check_balance(total_balance)

    if not balance_check.allowed:
        raise HTTPException(
            status_code=402,
            detail="Purchase credits to use Co-Pilot",
        )

    # Process through CoPilotService (LLM call + usage report)
    try:
        response_text, report = await copilot_service.process_message_metered(
            user=user,
            message=request.message,
            session_id=request.session_id,
            message_id=request.message_id,
        )
    except NotImplementedError:
        raise HTTPException(
            status_code=501,
            detail="CoPilot coming soon — use the streaming or agentic endpoints",
        )

    new_balance = report.new_balance_cents if report else total_balance
    cost = report.cost_cents if report else 0

    return BrainMessageResponse(
        message=response_text,
        balance_cents=new_balance,
        cost_cents=cost,
    )


# ---------------------------------------------------------------------------
# REST endpoint: POST /api/copilot/upload (BQ-ALLAI-FILES)
# ---------------------------------------------------------------------------

def _check_upload_rate(user_id: str) -> tuple[bool, str]:
    """Check upload rate limits. Returns (allowed, reason)."""
    now = time.monotonic()
    # Per-minute rate
    window = 60.0
    timestamps = _upload_rate[user_id]
    _upload_rate[user_id] = [t for t in timestamps if now - t < window]
    if len(_upload_rate[user_id]) >= MAX_UPLOADS_PER_MINUTE:
        return False, "Upload rate limit exceeded (max 5/min)"
    # Per-session total
    if _upload_session_count[user_id] >= MAX_UPLOADS_PER_SESSION:
        return False, "Upload session limit exceeded (max 20/session)"
    _upload_rate[user_id].append(now)
    _upload_session_count[user_id] += 1
    return True, ""


@router.post(
    "/upload",
    summary="Upload a file for use in allAI chat",
    description=(
        "Upload a file for ephemeral use in allAI chat. "
        "Files are stored for 1 hour and are NOT added to the dataset catalog."
    ),
    responses={
        413: {"description": "File too large"},
        415: {"description": "Unsupported file type"},
        429: {"description": "Upload rate limit exceeded"},
    },
)
async def copilot_upload(
    file: UploadFile = File(...),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Upload a file for use in allAI chat. Returns an attachment handle."""
    # Rate limit
    rate_ok, rate_reason = _check_upload_rate(user.user_id)
    if not rate_ok:
        raise HTTPException(status_code=429, detail=rate_reason)

    # Generate attachment ID
    attachment_id = f"att_{uuid.uuid4().hex[:12]}"

    # Sanitize filename
    original_name = file.filename or "upload"
    safe_name = sanitize_filename(original_name)

    # Create attachment directory
    from app.services.chat_attachment_service import CHAT_UPLOAD_DIR
    att_dir = CHAT_UPLOAD_DIR / attachment_id
    att_dir.mkdir(parents=True, exist_ok=True)
    file_path = att_dir / safe_name

    # Stream file to disk with byte counting (reject > 10MB early)
    total_bytes = 0
    header_bytes = b""
    try:
        with open(file_path, "wb") as f:
            while True:
                chunk = await file.read(64 * 1024)  # 64KB chunks
                if not chunk:
                    break
                if not header_bytes:
                    header_bytes = chunk[:32]
                total_bytes += len(chunk)
                if total_bytes > MAX_FILE_SIZE:
                    f.close()
                    import shutil
                    shutil.rmtree(att_dir, ignore_errors=True)
                    raise HTTPException(
                        status_code=413,
                        detail=f"File exceeds maximum size ({MAX_FILE_SIZE // (1024 * 1024)}MB)",
                    )
                f.write(chunk)
    except HTTPException:
        raise
    except Exception as e:
        import shutil
        shutil.rmtree(att_dir, ignore_errors=True)
        logger.error("Upload write failed: %s", e)
        raise HTTPException(status_code=500, detail="Upload failed")

    # Magic-byte MIME detection on header bytes
    mime_type = detect_mime_from_header(header_bytes)

    # For PK header: full-file ZIP detection after write
    if mime_type == "application/zip":
        mime_type = detect_mime_for_zip(file_path)

    # Validate against allowed types
    if mime_type is None or mime_type not in ALLOWED_MIME_TYPES:
        import shutil
        shutil.rmtree(att_dir, ignore_errors=True)
        raise HTTPException(
            status_code=415,
            detail="Unsupported file type",
        )

    # For images: validate + resize
    if mime_type.startswith("image/"):
        valid, reason = validate_image(file_path)
        if not valid:
            import shutil
            shutil.rmtree(att_dir, ignore_errors=True)
            raise HTTPException(status_code=415, detail=reason)

        resized = resize_if_needed(file_path)

        # Post-resize: reject if still > 4MB (M13)
        if resized:
            new_size = file_path.stat().st_size
            total_bytes = new_size  # Update for response
            if new_size > MAX_POST_RESIZE_BYTES:
                import shutil
                shutil.rmtree(att_dir, ignore_errors=True)
                raise HTTPException(
                    status_code=413,
                    detail="Image exceeds 4MB after resize. Use a lower-resolution image.",
                )

    # Store via ChatAttachmentService
    attachment = await chat_attachment_service.store(
        attachment_id=attachment_id,
        user_id=user.user_id,
        filename=safe_name,
        mime_type=mime_type,
        file_path=file_path,
        size_bytes=total_bytes,
    )

    # Log: attachment ID, type, size only. NEVER log content or full filename (M5, M11)
    logger.info(
        "Upload: att=%s type=%s size=%d name=%.30s",
        attachment.id, attachment.type, attachment.size_bytes, attachment.filename,
    )

    return attachment.to_response_dict()


# ---------------------------------------------------------------------------
# REST endpoint: GET /api/copilot/status
# ---------------------------------------------------------------------------

@router.get(
    "/status",
    response_model=CoPilotStatusResponse,
    summary="Get Co-Pilot session status for current user",
    description="Returns whether the current user has an active WebSocket session.",
)
async def get_copilot_status(
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Check if the authenticated user has an active co-pilot WebSocket session."""
    session_id = manager.get_session_for_user(user.user_id)

    if session_id and manager.get_ws(session_id):
        return CoPilotStatusResponse(
            active=True,
            session_id=session_id,
            connected_since=manager.get_connected_since(session_id),
        )

    # Clean up stale mapping if ws is gone
    if session_id:
        await manager.disconnect(session_id)

    return CoPilotStatusResponse(active=False)


# ---------------------------------------------------------------------------
# REST endpoints: Session History (BQ-128 Task 1.5)
# ---------------------------------------------------------------------------

class SessionCreateRequest(BaseModel):
    """Request model for creating a new session."""
    title: Optional[str] = None
    dataset_id: Optional[str] = None


@router.post(
    "/sessions",
    response_model=SessionRead,
    summary="Create a new chat session",
)
async def create_session(
    request: SessionCreateRequest,
    user: AuthenticatedUser = Depends(get_current_user),
    db: DBSession = Depends(get_legacy_session),
):
    """Create a new chat session for the authenticated user."""
    session = ChatSession(
        user_id=user.user_id,
        title=request.title or "Allie Chat",
        dataset_id=request.dataset_id,
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return session


@router.get(
    "/sessions",
    response_model=List[SessionRead],
    summary="List chat sessions for current user",
)
async def list_sessions(
    user: AuthenticatedUser = Depends(get_current_user),
    db: DBSession = Depends(get_legacy_session),
    limit: int = Query(default=20, ge=1, le=100),
):
    """List chat sessions, filtered by user_id (users only see their own)."""
    stmt = (
        select(ChatSession)
        .where(ChatSession.user_id == user.user_id, ChatSession.archived == False)  # noqa: E712
        .order_by(ChatSession.updated_at.desc())
        .limit(limit)
    )
    sessions = db.exec(stmt).all()
    return sessions


@router.get(
    "/sessions/current/messages",
    response_model=List[MessageRead],
    summary="Get messages for the user's most recent chat session",
)
async def get_current_session_messages(
    user: AuthenticatedUser = Depends(get_current_user),
    db: DBSession = Depends(get_legacy_session),
    limit: int = Query(default=50, ge=1, le=100),
):
    """
    Get messages for the user's most recent chat session.
    Scoped to user_id — users can only access their own sessions.
    Returns empty array if no session exists.
    """
    # Find most recent non-archived session for this user
    session_stmt = (
        select(ChatSession)
        .where(ChatSession.user_id == user.user_id, ChatSession.archived == False)  # noqa: E712
        .order_by(ChatSession.created_at.desc())
    )
    session = db.exec(session_stmt).first()
    if not session:
        return []

    # Get messages for this session
    msg_stmt = (
        select(Message)
        .where(Message.session_id == session.id)
        .order_by(Message.created_at.asc())
        .limit(limit)
    )
    messages = db.exec(msg_stmt).all()
    return messages


@router.get(
    "/sessions/{session_id}/messages",
    response_model=List[MessageRead],
    summary="Get messages for a specific session",
)
async def get_session_messages(
    session_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
    db: DBSession = Depends(get_legacy_session),
    limit: int = Query(default=50, ge=1, le=100),
):
    """
    Get messages for a specific session.
    Returns 404 if session doesn't belong to user (not 403 to avoid enumeration).
    """
    import uuid as uuid_mod
    try:
        sid = uuid_mod.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Session not found")

    session = db.get(ChatSession, sid)
    if not session or session.user_id != user.user_id:
        raise HTTPException(status_code=404, detail="Session not found")

    msg_stmt = (
        select(Message)
        .where(Message.session_id == session.id)
        .order_by(Message.created_at.asc())
        .limit(limit)
    )
    messages = db.exec(msg_stmt).all()
    return messages
