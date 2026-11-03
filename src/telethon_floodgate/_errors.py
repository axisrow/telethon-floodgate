"""Shared refusal error hierarchy root.

Lives in its own import-free module so that both ``rate_limit_gate`` (which
imports ``rate_limiter``) and ``rate_limiter`` itself can subclass the root
without a circular import (#11). ``rate_limit_gate`` re-exports the class, so
existing ``from telethon_floodgate.rate_limit_gate import
TelegramRateLimitedError`` keeps working unchanged.
"""


class TelegramRateLimitedError(RuntimeError):
    """Raised when an operation is deferred before making a Telegram call.

    Single root of the refusal hierarchy: every "operation unavailable, move
    on" handler in consumers catches this one type and covers the proactive
    gate, per-peer buckets, the breaker and the username-resolve limiter alike.
    Never introduce a sibling error type for a refusal.
    """

    def __init__(
        self,
        phone: str,
        category: str,
        retry_after_sec: float,
        *,
        message: str | None = None,
    ) -> None:
        # ``message`` lets refusal subclasses outside the gate keep their
        # historical message text byte-for-byte while still carrying the base
        # contract fields (phone/category/retry_after_sec) for shared handlers.
        if message is None:
            message = f"Telegram {category} rate-limited for {phone}; retry in {retry_after_sec:.1f}s"
        super().__init__(message)
        self.phone = phone
        self.category = category
        self.retry_after_sec = retry_after_sec
