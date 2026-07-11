class ExchangeRequestUncertainError(RuntimeError):
    """The exchange may have accepted a mutating request before losing its response."""
