from datetime import datetime, timedelta, timezone

import boto3
import pytest
from botocore.stub import Stubber

from app.models.dataset import DatasetRecord  # noqa: F401
from app.models.s3_connection import S3Connection
from app.models.s3_object_metadata import S3ObjectMetadata  # noqa: F401
from app.models.s3_scan_job import S3ScanJob  # noqa: F401
from app.services.sts_broker import STSAssumeError, STSBroker, STSConnectionNotReady


ROLE_ARN = "arn:aws:iam::210987654321:role/aim-data"
EXTERNAL_ID = "external-id-123"


@pytest.fixture(autouse=True)
def clear_sts_cache():
    STSBroker.clear_cache()
    yield
    STSBroker.clear_cache()


def _connection(**overrides) -> S3Connection:
    values = {
        "id": "seller-123",
        "name": "Seller bucket",
        "bucket": "seller-bucket",
        "region": "us-east-1",
        "role_arn": ROLE_ARN,
        "external_id": EXTERNAL_ID,
        "status": "configured",
    }
    values.update(overrides)
    return S3Connection(**values)


def _sts_client():
    return boto3.client(
        "sts",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        aws_session_token="test",
    )


def _assume_response(expiration: datetime, access_key: str = "ASIAIOSFODNN7EXAMPLE") -> dict:
    return {
        "Credentials": {
            "AccessKeyId": access_key,
            "SecretAccessKey": "secret-access-key",
            "SessionToken": "session-token",
            "Expiration": expiration,
        },
        "AssumedRoleUser": {
            "AssumedRoleId": "AROA123EXAMPLE:aim-seller-123-fulfillment",
            "Arn": ROLE_ARN,
        },
    }


def _expected_params(connection: S3Connection, role_session_name: str) -> dict:
    return {
        "RoleArn": connection.role_arn,
        "RoleSessionName": role_session_name,
        "ExternalId": connection.external_id,
        "DurationSeconds": 3600,
    }


def test_assume_role_success_returns_typed_credentials():
    connection = _connection()
    sts_client = _sts_client()
    stubber = Stubber(sts_client)
    expiration = datetime.now(timezone.utc) + timedelta(hours=1)
    stubber.add_response(
        "assume_role",
        _assume_response(expiration),
        _expected_params(connection, "aim-seller-123-fulfillment"),
    )
    stubber.activate()

    credentials = STSBroker(sts_client).assume_role(connection, "fulfillment")

    assert credentials.access_key_id == "ASIAIOSFODNN7EXAMPLE"
    assert credentials.secret_access_key == "secret-access-key"
    assert credentials.session_token == "session-token"
    assert credentials.expiration.tzinfo is not None
    assert credentials.expiration.utcoffset() == timedelta(0)
    assert credentials.region == "us-east-1"
    stubber.assert_no_pending_responses()


def test_assume_role_uses_cache_without_second_sts_call():
    connection = _connection()
    sts_client = _sts_client()
    stubber = Stubber(sts_client)
    expiration = datetime.now(timezone.utc) + timedelta(hours=1)
    stubber.add_response(
        "assume_role",
        _assume_response(expiration),
        _expected_params(connection, "aim-seller-123-fulfillment"),
    )
    stubber.activate()
    broker = STSBroker(sts_client)

    first = broker.assume_role(connection, "fulfillment")
    second = broker.assume_role(connection, "fulfillment")

    assert second is first
    stubber.assert_no_pending_responses()


def test_assume_role_refreshes_cache_inside_safety_margin():
    connection = _connection()
    sts_client = _sts_client()
    stubber = Stubber(sts_client)
    soon = datetime.now(timezone.utc) + timedelta(seconds=299)
    later = datetime.now(timezone.utc) + timedelta(hours=1)
    stubber.add_response(
        "assume_role",
        _assume_response(soon, access_key="ASIAIOSFODNN7EXAMPLE"),
        _expected_params(connection, "aim-seller-123-fulfillment"),
    )
    stubber.add_response(
        "assume_role",
        _assume_response(later, access_key="ASIAREFRESHEDKEY123"),
        _expected_params(connection, "aim-seller-123-fulfillment"),
    )
    stubber.activate()
    broker = STSBroker(sts_client)

    first = broker.assume_role(connection, "fulfillment")
    second = broker.assume_role(connection, "fulfillment")

    assert first.access_key_id == "ASIAIOSFODNN7EXAMPLE"
    assert second.access_key_id == "ASIAREFRESHEDKEY123"
    stubber.assert_no_pending_responses()


def test_distinct_purposes_do_not_collide_in_cache():
    connection = _connection()
    sts_client = _sts_client()
    stubber = Stubber(sts_client)
    expiration = datetime.now(timezone.utc) + timedelta(hours=1)
    stubber.add_response(
        "assume_role",
        _assume_response(expiration, access_key="ASIAPURPOSEONE1234"),
        _expected_params(connection, "aim-seller-123-order-1"),
    )
    stubber.add_response(
        "assume_role",
        _assume_response(expiration, access_key="ASIAPURPOSETWO1234"),
        _expected_params(connection, "aim-seller-123-order-2"),
    )
    stubber.activate()
    broker = STSBroker(sts_client)

    first = broker.assume_role(connection, "order-1")
    second = broker.assume_role(connection, "order-2")

    assert first.access_key_id == "ASIAPURPOSEONE1234"
    assert second.access_key_id == "ASIAPURPOSETWO1234"
    stubber.assert_no_pending_responses()


def test_access_denied_maps_to_sts_assume_error():
    connection = _connection()
    sts_client = _sts_client()
    stubber = Stubber(sts_client)
    stubber.add_client_error(
        "assume_role",
        service_error_code="AccessDenied",
        service_message="Cannot assume role",
        expected_params=_expected_params(connection, "aim-seller-123-fulfillment"),
    )
    stubber.activate()

    with pytest.raises(STSAssumeError) as exc_info:
        STSBroker(sts_client).assume_role(connection, "fulfillment")

    assert exc_info.value.aws_error_code == "AccessDenied"
    assert "trust policy" in str(exc_info.value)
    stubber.assert_no_pending_responses()


def test_not_configured_connection_raises_not_ready_without_sts_call():
    connection = _connection(status="onboarding", role_arn=None)
    sts_client = _sts_client()
    stubber = Stubber(sts_client)
    stubber.activate()

    with pytest.raises(STSConnectionNotReady):
        STSBroker(sts_client).assume_role(connection, "fulfillment")

    stubber.assert_no_pending_responses()


def test_role_session_name_sanitizes_and_truncates_with_distinct_hash_suffixes():
    first = STSBroker._role_session_name("seller/with spaces", "purpose/" + ("a" * 100))
    second = STSBroker._role_session_name("seller/with spaces", "purpose/" + ("b" * 100))

    assert len(first) == 64
    assert len(second) == 64
    assert first != second
    assert first.startswith("aim-seller-with-spaces-purpose")
    assert second.startswith("aim-seller-with-spaces-purpose")
    assert "/" not in first
    assert " " not in first
