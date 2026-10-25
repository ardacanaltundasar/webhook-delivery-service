import httpx
import pytest

from app.core.security import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    SignatureVerificationError,
    build_signature_headers,
    compute_signature,
    generate_secret,
    is_valid_api_key,
    verify_signature,
)

SECRET = "whsec_test_secret"
BODY = b'{"id":"evt_1","type":"order.created","data":{"order_id":42}}'
NOW = 1_760_000_000


def test_signature_roundtrip() -> None:
    headers = build_signature_headers(SECRET, BODY, timestamp=NOW)

    verify_signature(SECRET, BODY, headers[TIMESTAMP_HEADER], headers[SIGNATURE_HEADER], now=NOW)


def test_signature_format_is_stable() -> None:
    # Receivers implement this independently; changing the signed message breaks all of them.
    assert compute_signature(SECRET, NOW, BODY) == (
        "sha256=8ffe67309d2225355153373a7354c7810beb4ddec73231292601941f28f09b6a"
    )


def test_tampered_body_is_rejected() -> None:
    headers = build_signature_headers(SECRET, BODY, timestamp=NOW)

    with pytest.raises(SignatureVerificationError, match="mismatch"):
        verify_signature(
            SECRET, BODY.replace(b"42", b"43"), headers[TIMESTAMP_HEADER], headers[SIGNATURE_HEADER], now=NOW
        )


def test_signature_is_bound_to_timestamp() -> None:
    headers = build_signature_headers(SECRET, BODY, timestamp=NOW)

    with pytest.raises(SignatureVerificationError, match="mismatch"):
        verify_signature(SECRET, BODY, str(NOW + 60), headers[SIGNATURE_HEADER], now=NOW + 60)


def test_wrong_secret_is_rejected() -> None:
    headers = build_signature_headers(SECRET, BODY, timestamp=NOW)

    with pytest.raises(SignatureVerificationError):
        verify_signature("whsec_other", BODY, headers[TIMESTAMP_HEADER], headers[SIGNATURE_HEADER], now=NOW)


@pytest.mark.parametrize(
    ("clock_offset", "accepted"),
    [(0, True), (299, True), (-299, True), (301, False), (-301, False)],
)
def test_replay_window(clock_offset: int, accepted: bool) -> None:
    headers = build_signature_headers(SECRET, BODY, timestamp=NOW)

    def verify() -> None:
        verify_signature(
            SECRET,
            BODY,
            headers[TIMESTAMP_HEADER],
            headers[SIGNATURE_HEADER],
            tolerance_seconds=300,
            now=NOW + clock_offset,
        )

    if accepted:
        verify()
    else:
        with pytest.raises(SignatureVerificationError, match="tolerance"):
            verify()


@pytest.mark.parametrize(
    ("timestamp", "signature", "reason"),
    [
        (None, "sha256=abc", "Missing"),
        (str(NOW), None, "Missing"),
        ("not-a-number", "sha256=abc", "Malformed"),
        (str(NOW), "sha256=\u00e9", "mismatch"),
    ],
)
def test_malformed_headers_are_rejected(timestamp: str | None, signature: str | None, reason: str) -> None:
    with pytest.raises(SignatureVerificationError, match=reason):
        verify_signature(SECRET, BODY, timestamp, signature, now=NOW)


def test_generated_secrets_are_prefixed_and_unique() -> None:
    secrets = {generate_secret() for _ in range(100)}

    assert len(secrets) == 100
    assert all(secret.startswith("whsec_") and len(secret) > 40 for secret in secrets)


@pytest.mark.parametrize(
    ("provided", "expected"),
    [("key-123", True), ("key-124", False), ("", False), (None, False), ("k\u00e9y-123", False)],
)
def test_api_key_comparison(provided: str | None, expected: bool) -> None:
    assert is_valid_api_key(provided, "key-123") is expected


async def test_endpoints_require_api_key(client: httpx.AsyncClient) -> None:
    client.headers.pop("X-API-Key")

    missing = await client.get("/subscriptions")
    wrong = await client.get("/subscriptions", headers={"X-API-Key": "wrong"})

    assert missing.status_code == wrong.status_code == 401
    assert missing.headers["WWW-Authenticate"] == "ApiKey"
