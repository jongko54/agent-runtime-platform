import hashlib
import json
from typing import Any


def canonical_digest(value: Any) -> str:
    """Hash the supported JSON representation; not a general RFC 8785 encoder."""
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
