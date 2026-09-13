class DomainError(Exception):
    code = "DOMAIN_ERROR"


class InvalidTransition(DomainError):
    code = "INVALID_TRANSITION"
