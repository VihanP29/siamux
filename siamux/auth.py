"""High-entropy API keys: only public IDs and full-key digests are persisted."""

import hashlib
import hmac
import re
import secrets
from typing import Annotated
from uuid import uuid4

from fastapi import Header, HTTPException

from siamux.db import APIKey, session_factory

KEY_PATTERN = re.compile(r"sx_([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})\.([A-Za-z0-9_-]{43})")


def issue_key(session, tenant_id):
    public_id = str(uuid4())
    raw = f"sx_{public_id}.{secrets.token_urlsafe(32)}"
    session.add(
        APIKey(id=public_id, tenant_id=tenant_id, digest=hashlib.sha256(raw.encode()).hexdigest())
    )
    return raw


def authenticate(authorization: Annotated[str | None, Header()] = None) -> str:
    unauthorized = HTTPException(
        401,
        detail={"code": "UNAUTHORIZED", "message": "A valid API key is required."},
        headers={"WWW-Authenticate": "Bearer"},
    )
    if not authorization or not authorization.startswith("Bearer "):
        raise unauthorized
    raw = authorization[7:]
    match = KEY_PATTERN.fullmatch(raw)
    if not match:
        raise unauthorized
    with session_factory()() as session:
        key = session.get(APIKey, match[1])
        if key is None or key.revoked_at is not None:
            raise unauthorized
        if not hmac.compare_digest(key.digest, hashlib.sha256(raw.encode()).hexdigest()):
            raise unauthorized
        return key.tenant_id
