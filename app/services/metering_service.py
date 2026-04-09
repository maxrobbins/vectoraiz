"""
Metering Service — Pre-flight Balance Gate & Post-flight Usage Reporting
=========================================================================

PURPOSE:
    Manages the credit metering lifecycle for Co-Pilot (Claude) calls:
    1. **check_balance()** — Pre-flight: verifies the user's cached balance
       exceeds the estimated cost before allowing a Claude API call.
    2. **calculate_cost_cents()** — Converts token counts to customer cost
       using Anthropic wholesale pricing × configurable markup.
    3. **report_usage()** — Post-flight: computes actual cost from token
       counts, applies markup, and enqueues a deduction for exactly-once
       delivery to ai-market-backend's POST /api/v1/credits/deduct.

IDEMPOTENCY KEY FORMAT (BQ-113):
    deduct:v1:{SHA256(user_id|session_id|message_id)[:32]}
    When message_id absent: use session_id|timestamp_ms (generated once,
    not recomputed on retry).

COST CALCULATION:
    Anthropic Claude Sonnet wholesale pricing (as of 2026-02):
      - Input:  $3.00 per 1M tokens  → 0.0003 cents per token
      - Output: $15.00 per 1M tokens → 0.0015 cents per token

    Customer cost = wholesale cost × COPILOT_MARKUP_RATE (default 3.0x)
    Minimum charge: COPILOT_MIN_COST_CENTS (default 1 cent)

CONFIGURATION (env vars with VECTORAIZ_ prefix per project convention):
    VECTORAIZ_COPILOT_MARKUP_RATE              — Markup multiplier (default 3.0)
    VECTORAIZ_COPILOT_MIN_COST_CENTS           — Floor charge per query (default 1)
    VECTORAIZ_COPILOT_ESTIMATED_QUERY_COST_CENTS — For pre-flight estimate (default 3)
    VECTORAIZ_INTERNAL_API_KEY                 — Auth for ai-market internal endpoints

FAIL-OPEN POLICY:
    If ai-market-backend is unreachable or returns a non-402 error during
    report_usage(), we fail open (allow the next request). 402 insufficient_funds
    is treated as failed_terminal — the user is NOT allowed to continue.

PHASE: BQ-073 / BQ-113 — Exactly-Once Billing
CREATED: S94 (2026-02-06)
UPDATED: S120/BQ-113 (2026-02-12) — stable idempotency, 402 handling
"""

from __future__ import annotations

import hashlib
import math
import logging
import time
from dataclasses import dataclass
from typing import Optional

import httpx

from app.config import settings
from .deduction_queue import deduction_queue

logger = logging.getLogger(__name__)

__all__ = [
    "MeteringService",
    "BalanceCheck",
    "UsageReport",
    "metering_service",
    "CLAUDE_INPUT_COST_PER_TOKEN_CENTS",
    "CLAUDE_OUTPUT_COST_PER_TOKEN_CENTS",
]


# ---------------------------------------------------------------------------
# Anthropic Claude wholesale pricing (cents per token)
#
# Claude Sonnet 4 (2026-02):
#   Input:  $3.00 / 1M tokens = $0.000003 / token = 0.0003 cents / token
#   Output: $15.00 / 1M tokens = $0.000015 / token = 0.0015 cents / token
#
# Customer pays: wholesale × COPILOT_MARKUP_RATE (default 3.0x)
# ---------------------------------------------------------------------------
CLAUDE_INPUT_COST_PER_TOKEN_CENTS: float = 300.0 / 1_000_000   # 0.0003 cents/token
CLAUDE_OUTPUT_COST_PER_TOKEN_CENTS: float = 1500.0 / 1_000_000  # 0.0015 cents/token


@dataclass(frozen=True)
class UsageReport:
    """Result of a report_usage() call."""

    success: bool
    cost_cents: int
    new_balance_cents: int
    allowed: bool  # True = user can make more requests; False = balance exhausted


@dataclass(frozen=True)
class BalanceCheck:
    """Result of a check_balance() call."""

    allowed: bool
    balance_cents: int
    estimated_cost_cents: int
    reason: Optional[str] = None


def _make_idempotency_key(
    user_id: str,
    session_id: str,
    message_id: Optional[str],
) -> str:
    """
    Build a stable idempotency key for a deduction.

    Format: deduct:v1:{SHA256(user_id|session_id|message_id)[:32]}
    When message_id is absent: use session_id|timestamp_ms (generated once).
    """
    if message_id:
        raw = f"{user_id}|{session_id}|{message_id}"
    else:
        # Fallback: one-time timestamp — generated at call time, stable across retries
        # because it is stored in the queue payload once
        ts_ms = int(time.time() * 1000)
        raw = f"{user_id}|{session_id}|{ts_ms}"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:32]
    return f"deduct:v1:{digest}"


class MeteringService:
    """
    Metering service for Co-Pilot credit management.

    Handles pre-flight balance verification and post-flight usage
    reporting to ai-market-backend.
    """

    def __init__(self) -> None:
        self._markup_rate: float = settings.copilot_markup_rate
        self._min_cost_cents: int = settings.copilot_min_cost_cents
        self._estimated_query_cost: int = settings.copilot_estimated_query_cost_cents

    # ------------------------------------------------------------------
    # Properties for read-only access to config
    # ------------------------------------------------------------------

    @property
    def markup_rate(self) -> float:
        return self._markup_rate

    @property
    def min_cost_cents(self) -> int:
        return self._min_cost_cents

    @property
    def estimated_query_cost(self) -> int:
        return self._estimated_query_cost

    # ------------------------------------------------------------------
    # Pre-flight: check_balance
    # ------------------------------------------------------------------

    def check_balance(
        self,
        balance_cents: int,
        estimated_cost_cents: Optional[int] = None,
    ) -> BalanceCheck:
        """
        Verify that the user's cached balance is sufficient for a Claude call.
        """
        if not settings.auth_enabled:
            return BalanceCheck(
                allowed=True,
                balance_cents=balance_cents,
                estimated_cost_cents=0,
                reason="auth_disabled",
            )

        cost = estimated_cost_cents if estimated_cost_cents is not None else self._estimated_query_cost

        if balance_cents <= 0:
            return BalanceCheck(
                allowed=False,
                balance_cents=balance_cents,
                estimated_cost_cents=cost,
                reason="zero_balance",
            )

        if balance_cents < cost:
            return BalanceCheck(
                allowed=False,
                balance_cents=balance_cents,
                estimated_cost_cents=cost,
                reason="insufficient_balance",
            )

        return BalanceCheck(
            allowed=True,
            balance_cents=balance_cents,
            estimated_cost_cents=cost,
        )

    # ------------------------------------------------------------------
    # Cost calculation
    # ------------------------------------------------------------------

    def calculate_cost_cents(
        self,
        input_tokens: int,
        output_tokens: int,
        markup_rate: Optional[float] = None,
    ) -> int:
        """
        Calculate the customer cost in cents for a Claude API call.
        """
        if input_tokens < 0 or output_tokens < 0:
            raise ValueError(
                f"Token counts must be non-negative: "
                f"input_tokens={input_tokens}, output_tokens={output_tokens}"
            )

        rate = markup_rate if markup_rate is not None else self._markup_rate

        wholesale_cents = (
            input_tokens * CLAUDE_INPUT_COST_PER_TOKEN_CENTS
            + output_tokens * CLAUDE_OUTPUT_COST_PER_TOKEN_CENTS
        )

        customer_cents = math.ceil(wholesale_cents * rate)

        if (input_tokens > 0 or output_tokens > 0) and customer_cents < self._min_cost_cents:
            customer_cents = self._min_cost_cents

        return customer_cents

    # ------------------------------------------------------------------
    # Post-flight: report_usage
    # ------------------------------------------------------------------

    async def report_usage(
        self,
        user_id: str,
        service: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        session_id: str,
        message_id: Optional[str] = None,
        markup_rate: Optional[float] = None,
    ) -> UsageReport:
        """
        Report token usage to ai-market-backend and deduct credits.

        Called AFTER the Claude API call completes. Computes the cost
        from actual token counts, then enqueues a deduction with a stable
        idempotency key for exactly-once delivery.

        Args:
            user_id: The user's UUID.
            service: Service identifier (e.g. "copilot").
            model: Model identifier (e.g. "claude-sonnet-4-20250514").
            input_tokens: Actual input tokens consumed.
            output_tokens: Actual output tokens generated.
            session_id: Session/conversation identifier for idempotency.
            message_id: Optional message identifier for idempotency.
            markup_rate: Override markup rate.

        Returns:
            UsageReport with success status, cost, new balance, and whether
            the user is still allowed to make more requests.

        Note:
            On network/server errors, we **fail open** — the report returns
            ``success=False`` but ``allowed=True``.
            On 402 insufficient_funds, ``allowed=False``.
        """
        if not settings.auth_enabled:
            logger.debug("Auth disabled — skipping usage report")
            return UsageReport(
                success=True,
                cost_cents=0,
                new_balance_cents=999_999,
                allowed=True,
            )

        rate = markup_rate if markup_rate is not None else self._markup_rate
        cost_cents = self.calculate_cost_cents(input_tokens, output_tokens, rate)

        if cost_cents == 0:
            logger.debug(
                "Zero-cost call (0 tokens) — skipping usage report for user=%s",
                user_id,
            )
            return UsageReport(
                success=True,
                cost_cents=0,
                new_balance_cents=0,
                allowed=True,
            )

        # Generate stable idempotency key
        idempotency_key = _make_idempotency_key(user_id, session_id, message_id)

        payload = {
            "user_id": user_id,
            "amount_cents": cost_cents,
            "service": service,
            "tokens_in": input_tokens,
            "tokens_out": output_tokens,
            "model": model,
            "markup_rate": rate,
            "idempotency_key": idempotency_key,
        }

        logger.info(
            "Reporting usage: user=%s service=%s model=%s "
            "tokens_in=%d tokens_out=%d cost=%d¢ markup=%.1fx idem=%s",
            user_id, service, model,
            input_tokens, output_tokens,
            cost_cents, rate, idempotency_key,
        )

        # Enqueue for persistence (exactly-once via idempotency_key)
        deduction_queue.enqueue(payload)

        # Attempt synchronous deduction for fast feedback
        success, data, retryable, status_code = await self._attempt_deduct(
            payload, idempotency_key
        )

        if success:
            new_balance = data.get(
                "balance_cents",
                data.get("detail", {}).get("balance_cents", 0),
            )
            allowed = new_balance > 0
            deduction_queue.mark_completed(idempotency_key)
            logger.info(
                "Usage deducted: user=%s cost=%d¢ new_balance=%d¢",
                user_id, cost_cents, new_balance,
            )
            return UsageReport(True, cost_cents, new_balance, allowed)

        # 402 insufficient_funds → failed_terminal, NOT allowed
        if status_code == 402:
            new_balance = data.get(
                "detail", {},
            ).get("balance_cents", 0) if isinstance(data.get("detail"), dict) else 0
            deduction_queue.mark_failed_terminal(
                idempotency_key, "insufficient_funds"
            )
            logger.warning(
                "Insufficient funds: user=%s status=402 balance=%d¢",
                user_id, new_balance,
            )
            return UsageReport(False, cost_cents, new_balance, False)

        # Other non-retryable client errors (400/401/403/404) → terminal, fail open
        if status_code in (400, 401, 403, 404):
            deduction_queue.mark_failed_terminal(
                idempotency_key, f"HTTP {status_code}"
            )
            logger.error(
                "Permanent failure deducting usage: user=%s status=%d",
                user_id, status_code,
            )
            return UsageReport(False, cost_cents, 0, True)

        # Retryable errors (5xx, timeout, network) → leave in queue, fail open
        logger.warning(
            "Retryable failure deducting usage: user=%s status=%d — left in queue",
            user_id, status_code,
        )
        return UsageReport(False, cost_cents, 0, True)

    async def _attempt_deduct(
        self,
        payload: dict,
        idempotency_key: str,
    ) -> tuple[bool, dict, bool, int]:
        """
        Attempt to send the deduction request to ai-market-backend.

        Returns (success, response_data, retryable, status_code).

        Classification matches deduction_queue._attempt_send:
          - 200         → success
          - 402         → not success, not retryable (insufficient_funds)
          - 400/401/403/404 → not success, not retryable (client error)
          - 5xx         → not success, retryable
          - timeout/network → not success, retryable
          - JSON parse on non-5xx → not success, not retryable
          - JSON parse on 5xx → not success, retryable
        """
        url = f"{settings.ai_market_url}/api/v1/credits/deduct"

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        }
        if settings.internal_api_key:
            headers["X-Internal-API-Key"] = settings.internal_api_key

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, headers=headers, json=payload)

            status_code = response.status_code

            try:
                data = response.json()
            except (ValueError, Exception):
                if 500 <= status_code < 600:
                    logger.error(
                        "JSON parse failure on 5xx from ai-market: status=%d",
                        status_code,
                    )
                    return False, {}, True, status_code
                else:
                    logger.error(
                        "JSON parse failure on non-5xx from ai-market: status=%d",
                        status_code,
                    )
                    return False, {}, False, status_code

            if status_code == 200:
                return True, data, False, status_code

            if status_code == 402:
                return False, data, False, status_code

            if status_code in (400, 401, 403, 404):
                logger.error(
                    "Client error from ai-market deduct: status=%d body=%s",
                    status_code, response.text[:200],
                )
                return False, data, False, status_code

            if 500 <= status_code < 600:
                logger.error(
                    "5xx error from ai-market deduct: status=%d body=%s",
                    status_code, response.text[:200],
                )
                return False, data, True, status_code

            logger.error(
                "Unexpected status from ai-market deduct: status=%d",
                status_code,
            )
            return False, data, False, status_code

        except httpx.TimeoutException:
            logger.error("Timeout reporting usage to ai-market")
            return False, {}, True, 0

        except Exception as exc:
            logger.error("Error reporting usage to ai-market: error=%s", exc)
            return False, {}, True, 0


# Module-level singleton
metering_service = MeteringService()
