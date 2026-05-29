"""
Presigned S3 GET URL generation for seller-owned S3 objects.
"""

from __future__ import annotations

import boto3

from app.services.sts_broker import AssumedCredentials


def generate_presigned_get(
    creds: AssumedCredentials,
    bucket: str,
    key: str,
    expires_in: int = 900,
) -> str:
    """Generate a short-lived GET URL using assumed seller credentials."""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=creds.access_key_id,
        aws_secret_access_key=creds.secret_access_key,
        aws_session_token=creds.session_token,
        region_name=creds.region,
    )
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )
