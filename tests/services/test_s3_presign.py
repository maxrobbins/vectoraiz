from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, unquote, urlparse

from app.services.s3_presign import generate_presigned_get
from app.services.sts_broker import AssumedCredentials


def test_generate_presigned_get_returns_url_with_bucket_key_and_expiry():
    creds = AssumedCredentials(
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_access_key="secret-access-key",
        session_token="session-token",
        expiration=datetime.now(timezone.utc) + timedelta(hours=1),
        region="us-east-1",
    )

    before = int(datetime.now(timezone.utc).timestamp())
    url = generate_presigned_get(
        creds,
        "seller-bucket",
        "exports/folder/object.csv",
        expires_in=900,
    )
    after = int(datetime.now(timezone.utc).timestamp())

    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.netloc.startswith("seller-bucket.")
    assert unquote(parsed.path) == "/exports/folder/object.csv"
    if "X-Amz-Expires" in query:
        assert query["X-Amz-Expires"] == ["900"]
    else:
        expires_at = int(query["Expires"][0])
        assert before + 900 <= expires_at <= after + 900
