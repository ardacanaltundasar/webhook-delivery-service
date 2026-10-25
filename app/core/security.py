import hashlib
import hmac
import secrets
import time

SIGNATURE_HEADER = "X-Webhook-Signature-256"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
SIGNATURE_PREFIX = "sha256="
SECRET_PREFIX = "whsec_"


class SignatureVerificationError(Exception):
    pass


def generate_secret() -> str:
    return f"{SECRET_PREFIX}{secrets.token_urlsafe(32)}"


def is_valid_api_key(provided: str | None, expected: str) -> bool:
    if not provided:
        return False
    return secrets.compare_digest(provided.encode(), expected.encode())


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    # The timestamp is part of the signed message, so an attacker who captures a request
    # cannot replay it later with a fresh timestamp header without invalidating the MAC.
    message = f"{timestamp}.".encode() + body
    digest = hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
    return f"{SIGNATURE_PREFIX}{digest}"


def build_signature_headers(secret: str, body: bytes, timestamp: int | None = None) -> dict[str, str]:
    ts = int(time.time()) if timestamp is None else timestamp
    return {
        TIMESTAMP_HEADER: str(ts),
        SIGNATURE_HEADER: compute_signature(secret, ts, body),
    }


def verify_signature(
    secret: str,
    body: bytes,
    timestamp_header: str | None,
    signature_header: str | None,
    tolerance_seconds: int = 300,
    now: int | None = None,
) -> None:
    if not timestamp_header or not signature_header:
        raise SignatureVerificationError("Missing signature headers")

    try:
        timestamp = int(timestamp_header)
    except ValueError:
        raise SignatureVerificationError("Malformed timestamp header") from None

    current = int(time.time()) if now is None else now
    if abs(current - timestamp) > tolerance_seconds:
        raise SignatureVerificationError("Timestamp outside tolerance window")

    expected = compute_signature(secret, timestamp, body)
    if not hmac.compare_digest(expected.encode(), signature_header.encode()):
        raise SignatureVerificationError("Signature mismatch")
