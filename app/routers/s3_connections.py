"""
S3 Connection Router
====================

Customer-facing S3 STS connection setup and verification endpoints.
"""

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field
from sqlmodel import select

from app.config import settings
from app.core.database import get_session_context
from app.models.s3_connection import S3Connection

router = APIRouter()

ROLE_ARN_RE = re.compile(r"^arn:aws:iam::\d{12}:role/.+$")


class S3ConnectionCreate(BaseModel):
    name: str = Field(..., max_length=255)
    bucket: str = Field(..., max_length=255)
    region: str = Field(..., max_length=64)
    prefix: Optional[str] = Field(default=None, max_length=512)


class S3ConnectionRoleArn(BaseModel):
    role_arn: str


class S3ConnectionResponse(BaseModel):
    id: str
    name: str
    bucket: str
    region: str
    prefix: Optional[str] = None
    role_arn: Optional[str] = None
    external_id: Optional[str] = None
    status: str
    error_message: Optional[str] = None
    last_scanned_at: Optional[str] = None
    created_at: str
    updated_at: str
    trust_policy: Optional[Dict[str, Any]] = None
    permission_policy: Optional[Dict[str, Any]] = None


class S3VerifyResponse(BaseModel):
    status: str
    error_message: Optional[str] = None
    verified_at: Optional[str] = None


class S3ConfigResponse(BaseModel):
    aws_account_id: str


def _policy_prefix(prefix: Optional[str]) -> str:
    return prefix or ""


def _trust_policy(connection: S3Connection) -> Dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{settings.ai_market_aws_account_id}:root",
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": connection.external_id,
                    },
                },
            }
        ],
    }


def _permission_policy(connection: S3Connection) -> Dict[str, Any]:
    prefix = _policy_prefix(connection.prefix)
    list_statement: Dict[str, Any] = {
        "Effect": "Allow",
        "Action": ["s3:ListBucket"],
        "Resource": f"arn:aws:s3:::{connection.bucket}",
    }
    get_resource = f"arn:aws:s3:::{connection.bucket}/*"
    if prefix:
        list_statement["Condition"] = {
            "StringLike": {
                "s3:prefix": [f"{prefix}*"],
            },
        }
        get_resource = f"arn:aws:s3:::{connection.bucket}/{prefix}*"

    return {
        "Version": "2012-10-17",
        "Statement": [
            list_statement,
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": get_resource,
            },
        ],
    }


def _to_response(connection: S3Connection, include_policies: bool = False) -> S3ConnectionResponse:
    response = S3ConnectionResponse(
        id=connection.id,
        name=connection.name,
        bucket=connection.bucket,
        region=connection.region,
        prefix=connection.prefix,
        role_arn=connection.role_arn,
        external_id=connection.external_id,
        status=connection.status,
        error_message=connection.error_message,
        last_scanned_at=connection.last_scanned_at.isoformat() if connection.last_scanned_at else None,
        created_at=connection.created_at.isoformat(),
        updated_at=connection.updated_at.isoformat(),
    )
    if include_policies:
        response.trust_policy = _trust_policy(connection)
        response.permission_policy = _permission_policy(connection)
    return response


def _boto3_client(service_name: str, **kwargs):
    import boto3

    return boto3.client(service_name, **kwargs)


@router.get("/config", summary="Get S3 connection setup config")
async def get_config() -> S3ConfigResponse:
    return S3ConfigResponse(aws_account_id=settings.ai_market_aws_account_id)


@router.post("/", status_code=201, summary="Create S3 connection")
async def create_connection(body: S3ConnectionCreate) -> S3ConnectionResponse:
    connection = S3Connection(
        id=str(uuid.uuid4()),
        name=body.name,
        bucket=body.bucket,
        region=body.region,
        prefix=body.prefix or None,
        external_id=str(uuid.uuid4()),
        status="onboarding",
    )
    with get_session_context() as session:
        session.add(connection)
        session.commit()
        session.refresh(connection)
        return _to_response(connection, include_policies=True)


@router.get("/", summary="List S3 connections")
async def list_connections() -> List[S3ConnectionResponse]:
    with get_session_context() as session:
        rows = session.exec(select(S3Connection)).all()
        return [_to_response(row) for row in rows]


@router.get("/{connection_id}", summary="Get S3 connection")
async def get_connection(connection_id: str) -> S3ConnectionResponse:
    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")
        return _to_response(connection, include_policies=True)


@router.put("/{connection_id}/role-arn", summary="Set S3 role ARN")
async def set_role_arn(connection_id: str, body: S3ConnectionRoleArn) -> S3ConnectionResponse:
    if not ROLE_ARN_RE.match(body.role_arn):
        raise HTTPException(status_code=400, detail="Invalid IAM role ARN")

    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")

        connection.role_arn = body.role_arn
        connection.status = "configured"
        connection.error_message = None
        connection.updated_at = datetime.now(timezone.utc)
        session.add(connection)
        session.commit()
        session.refresh(connection)
        return _to_response(connection)


@router.post("/{connection_id}/verify", summary="Verify S3 connection")
async def verify_connection(connection_id: str) -> S3VerifyResponse:
    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")
        if not connection.role_arn:
            raise HTTPException(status_code=400, detail="S3 connection role ARN is not configured")

        try:
            sts_client = _boto3_client("sts", region_name=connection.region)
            assumed = sts_client.assume_role(
                RoleArn=connection.role_arn,
                RoleSessionName="aim-data-verify",
                ExternalId=connection.external_id,
            )
            credentials = assumed["Credentials"]
            s3_client = _boto3_client(
                "s3",
                region_name=connection.region,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
            )
            s3_client.list_objects_v2(
                Bucket=connection.bucket,
                Prefix=connection.prefix or "",
                MaxKeys=1,
            )
        except Exception as exc:
            connection.status = "error"
            connection.error_message = str(exc)
            connection.updated_at = datetime.now(timezone.utc)
            session.add(connection)
            session.commit()
            return S3VerifyResponse(status=connection.status, error_message=connection.error_message)

        verified_at = datetime.now(timezone.utc)
        connection.status = "verified"
        connection.last_scanned_at = verified_at
        connection.error_message = None
        connection.updated_at = verified_at
        session.add(connection)
        session.commit()
        return S3VerifyResponse(status=connection.status, verified_at=verified_at.isoformat())


@router.delete("/{connection_id}", status_code=204, summary="Delete S3 connection")
async def delete_connection(connection_id: str) -> Response:
    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")
        session.delete(connection)
        session.commit()
    return Response(status_code=204)
