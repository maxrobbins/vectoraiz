"""
STS AssumeRole broker for seller-owned S3 connections.
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import ClassVar

import boto3
from botocore.exceptions import ClientError

from app.models.s3_connection import S3Connection

logger = logging.getLogger(__name__)

_ROLE_SESSION_ALLOWED_RE = re.compile(r"[^A-Za-z0-9_+=,.@-]+")
_CACHE_SAFETY_MARGIN_SECONDS = 300
_ASSUME_ROLE_DURATION_SECONDS = 3600
_MAX_ROLE_SESSION_NAME_LENGTH = 64


@dataclass(frozen=True)
class AssumedCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime
    region: str


class STSConnectionNotReady(RuntimeError):
    """Raised when an S3 connection is not ready for STS."""


class STSAssumeError(RuntimeError):
    """Raised when AWS STS rejects an AssumeRole request."""

    def __init__(self, message: str, aws_error_code: str | None = None) -> None:
        super().__init__(message)
        self.aws_error_code = aws_error_code


class STSBroker:
    _cache: ClassVar[dict[tuple[str, str], AssumedCredentials]] = {}
    _cache_lock: ClassVar[threading.RLock] = threading.RLock()

    def __init__(self, sts_client=None) -> None:
        self._sts_client = sts_client

    def assume_role(self, connection: S3Connection, purpose: str) -> AssumedCredentials:
        if connection.status not in {"configured", "verified"} or connection.role_arn is None or connection.external_id is None:
            raise STSConnectionNotReady(
                "S3 connection is not configured. Complete role ARN and ExternalId setup before retrying."
            )

        role_session_name = self._role_session_name(connection.id, purpose)
        cache_key = (connection.id, role_session_name)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        logger.info(
            "sts_assume_role",
            extra={
                "connection_id": connection.id,
                "role_session_name": role_session_name,
                "purpose": purpose,
                "outcome": "attempt",
            },
        )
        try:
            response = self._client(connection.region).assume_role(
                RoleArn=connection.role_arn,
                RoleSessionName=role_session_name,
                ExternalId=connection.external_id,
                DurationSeconds=_ASSUME_ROLE_DURATION_SECONDS,
            )
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            logger.info(
                "sts_assume_role",
                extra={
                    "connection_id": connection.id,
                    "role_session_name": role_session_name,
                    "purpose": purpose,
                    "outcome": "error",
                    "aws_error_code": error_code,
                },
            )
            # 'from None' prevents the raw botocore ClientError surfacing in tracebacks
            # (MED, S728 security review): leak no AWS internals beyond the mapped error code.
            raise STSAssumeError(self._seller_actionable_message(error_code), error_code) from None

        credentials = response["Credentials"]
        assumed = AssumedCredentials(
            access_key_id=credentials["AccessKeyId"],
            secret_access_key=credentials["SecretAccessKey"],
            session_token=credentials["SessionToken"],
            expiration=self._utc_aware(credentials["Expiration"]),
            region=connection.region,
        )
        with self._cache_lock:
            self._cache[cache_key] = assumed

        logger.info(
            "sts_assume_role",
            extra={
                "connection_id": connection.id,
                "role_session_name": role_session_name,
                "purpose": purpose,
                "outcome": "success",
            },
        )
        return assumed

    @classmethod
    def clear_cache(cls) -> None:
        with cls._cache_lock:
            cls._cache.clear()

    @classmethod
    def _get_cached(cls, cache_key: tuple[str, str]) -> AssumedCredentials | None:
        with cls._cache_lock:
            cached = cls._cache.get(cache_key)
            if cached and (cached.expiration - datetime.now(timezone.utc)).total_seconds() > _CACHE_SAFETY_MARGIN_SECONDS:
                return cached
            if cached:
                cls._cache.pop(cache_key, None)
            return None

    def _client(self, region: str):
        if self._sts_client is not None:
            return self._sts_client
        return boto3.client("sts", region_name=region)

    @staticmethod
    def _role_session_name(seller_id: str, purpose: str) -> str:
        raw_name = f"aim-{seller_id}-{purpose}"
        sanitized = _ROLE_SESSION_ALLOWED_RE.sub("-", raw_name).strip("-")
        if len(sanitized) < 2:
            sanitized = f"aim-{sanitized}"
        if len(sanitized) <= _MAX_ROLE_SESSION_NAME_LENGTH:
            return sanitized

        digest = hashlib.sha256(raw_name.encode("utf-8")).hexdigest()[:12]
        suffix = f"-{digest}"
        prefix_length = _MAX_ROLE_SESSION_NAME_LENGTH - len(suffix)
        return f"{sanitized[:prefix_length].rstrip('-')}{suffix}"

    @staticmethod
    def _utc_aware(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _seller_actionable_message(error_code: str | None) -> str:
        if error_code == "AccessDenied":
            return (
                "AWS denied the AssumeRole request. Confirm the seller IAM role trust policy allows "
                "the platform AWS account and the configured ExternalId."
            )
        if error_code == "ExpiredToken":
            return "The platform AWS credentials used to call STS are expired. Refresh platform AWS credentials and retry."
        return (
            "AWS could not assume the seller IAM role. Confirm the role ARN, trust policy, ExternalId, "
            "and role session duration are configured correctly."
        )
