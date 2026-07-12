class ExchangeRequestUncertainError(RuntimeError):
    """The exchange may have accepted a mutating request before losing its response."""


class ExchangeRateLimitError(RuntimeError):
    """The exchange rejected work because a request or order limit was reached."""

    def __init__(self, message: str, *, retry_after: float = 60.0):
        super().__init__(message)
        self.retry_after = max(1.0, float(retry_after or 60.0))


def is_exchange_rate_limit_message(message: object) -> bool:
    normalized = str(message or "").lower()
    return any(
        hint in normalized
        for hint in (
            "too many new orders",
            "too many requests",
            "request rate limit",
            "order rate limit",
            "rate limit exceeded",
            "rate limit reached",
            "current limit of ip",
            "requests per minute",
            "orders per minute",
            "request weight",
            "ip banned",
            "auto-banned",
        )
    )
