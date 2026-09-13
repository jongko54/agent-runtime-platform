"""Safe errors crossing application boundaries; never carry provider payloads."""


class RetryableGatewayError(Exception):
    """Explicit transient mock/provider error; never used for persistence errors."""


class ApplicationError(Exception):
    code = "APPLICATION_ERROR"
    retryable = False

    def __init__(self, message: str = "Request could not be processed") -> None:
        self.message = message
        super().__init__(message)


class IdempotencyConflict(ApplicationError):
    code = "IDEMPOTENCY_CONFLICT"


class ExecutionScopeNotFound(ApplicationError):
    code = "EXECUTION_SCOPE_NOT_FOUND"


class InvalidInput(ApplicationError):
    code = "CLIENT_INVALID"


class PolicyDenied(ApplicationError):
    code = "POLICY_DENIED"


class AuthenticationRequired(ApplicationError):
    code = "AUTHENTICATION_REQUIRED"


class IdentityProviderNotConfigured(ApplicationError):
    code = "IDENTITY_PROVIDER_NOT_CONFIGURED"


class RuntimeConflict(ApplicationError):
    code = "RUNTIME_CONFLICT"


class ProviderUnavailable(ApplicationError):
    code = "PROVIDER_UNAVAILABLE"
