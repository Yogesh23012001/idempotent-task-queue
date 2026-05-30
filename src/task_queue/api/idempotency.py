"""Idempotency utilities."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_request_hash(body: dict[str, Any]) -> str:
    """Compute a stable hash of the request body.

    Same logical payload → same hash, regardless of key ordering or
    insignificant whitespace. SHA-256 hex, 64 chars.
    """
    canonical_json = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()   