"""Explicitly injected identity adapters; no implicit trust of client scope fields."""

import hmac
from collections.abc import Mapping

from agent_platform.application.errors import AuthenticationRequired
from agent_platform.application.ports import PrincipalContext


class StaticTokenVerifier:
    """Development/test adapter only, explicitly enabled by the composition root."""

    def __init__(self, tokens: Mapping[str, PrincipalContext]) -> None:
        self._tokens = dict(tokens)

    async def verify(self, bearer_token: str) -> PrincipalContext:
        for token, principal in self._tokens.items():
            if hmac.compare_digest(bearer_token.encode(), token.encode()):
                return principal
        raise AuthenticationRequired("Invalid credentials")
