# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Retry helper for transient network errors."""

import logging
import time

import requests.exceptions

logger = logging.getLogger(__name__)

TRANSIENT_ERRORS = (
    ConnectionError,
    ConnectionResetError,
    TimeoutError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def _is_rate_limited(exc: Exception) -> bool:
    """Return True if exc is an HTTP 429 Too Many Requests response."""
    http_err = getattr(exc, "response", None)
    return http_err is not None and getattr(http_err, "status_code", None) == 429


def retry_on_transient(fn, *args, max_retries=3, **kwargs):
    """Call fn(*args, **kwargs) with retry on transient network errors and 429s.

    Retries up to max_retries times with exponential backoff (2s, 4s, 8s).
    HTTP 429 (rate limit) responses are also retried with the same backoff.
    Re-raises the exception after exhausting retries.
    """
    for attempt in range(max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            is_transient = isinstance(exc, TRANSIENT_ERRORS)
            is_rate_limit = _is_rate_limited(exc)
            if not is_transient and not is_rate_limit:
                raise
            if attempt == max_retries:
                raise
            delay = 2 ** (attempt + 1)
            reason = "Rate limited" if is_rate_limit else "Transient error"
            logger.warning(
                "%s (attempt %d/%d), retrying in %ds: %s",
                reason, attempt + 1, max_retries, delay, exc,
            )
            time.sleep(delay)
