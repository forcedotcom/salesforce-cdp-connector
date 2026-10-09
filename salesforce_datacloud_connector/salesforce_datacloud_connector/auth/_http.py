"""
Shared HTTP plumbing for the OAuth and token-exchange endpoints.

Provides error-body parsing and retry with exponential backoff, mirroring the
JDBC driver's DataCloudTokenProvider (Failsafe policy: 1s initial delay,
doubling up to 30s, ``http.maxRetries`` retries, default 3).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

from ..exceptions import OperationalError


@dataclass(frozen=True)
class RetryConfig:
    """
    Retry policy for token endpoints.

    Attributes:
        max_retries: Retries after the first attempt (0 disables retrying).
        initial_backoff_seconds: Delay before the first retry; doubles each retry.
        max_backoff_seconds: Upper bound on the delay between retries.
    """

    max_retries: int = 3
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0

    def __post_init__(self):
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.initial_backoff_seconds < 0 or self.max_backoff_seconds < 0:
            raise ValueError("backoff seconds must be >= 0")

    def delay(self, retry_number: int) -> float:
        """Delay before retry ``retry_number`` (1-based)."""
        return min(
            self.initial_backoff_seconds * (2 ** (retry_number - 1)),
            self.max_backoff_seconds,
        )


DEFAULT_RETRY = RetryConfig()


def _is_retriable_status(status_code: int) -> bool:
    # 4xx other than 429 (400/401/403 ...) are credential/request problems
    # that retrying cannot fix.
    return status_code == 429 or 500 <= status_code < 600


def describe_error_response(response: requests.Response) -> str:
    """
    Build a readable message from a failed OAuth/token-exchange response.

    Salesforce returns ``{"error": "invalid_grant", "error_description": "..."}``;
    surface those instead of the bare "400 Client Error" from raise_for_status().
    """
    detail = ""
    try:
        body = response.json()
    except ValueError:
        body = None

    if isinstance(body, dict):
        error = body.get("error")
        description = body.get("error_description")
        if error and description:
            detail = f"{error}: {description}"
        else:
            detail = str(error or description or "")
    if not detail:
        detail = (response.text or "").strip()[:500]

    reason = f"HTTP {response.status_code}"
    return f"{reason} - {detail}" if detail else reason


def post_with_retry(
    url: str,
    *,
    description: str,
    retry: Optional[RetryConfig] = None,
    data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 30,
) -> Dict[str, Any]:
    """
    POST to a token endpoint and return the parsed JSON body.

    Retries 5xx, 429 and connection/timeout errors with exponential backoff.
    Other 4xx responses fail immediately.

    Args:
        url: Endpoint URL
        description: Prefix for error messages (e.g. "Authentication failed with JWT")
        retry: Retry policy (default: 3 retries, 1-30s exponential backoff)
        data: Form-encoded body
        params: Query-string parameters
        timeout: Per-request timeout in seconds

    Raises:
        OperationalError: On non-retriable failure or when retries are exhausted;
            the message includes the server's error/error_description when present.
    """
    retry = retry or DEFAULT_RETRY
    attempt = 0
    while True:
        try:
            response = requests.post(url, data=data, params=params, timeout=timeout)
        except requests.exceptions.RequestException as e:
            if attempt < retry.max_retries:
                attempt += 1
                time.sleep(retry.delay(attempt))
                continue
            raise OperationalError(f"{description}: {e}") from e

        if response.ok:
            try:
                return response.json()
            except ValueError as e:
                raise OperationalError(
                    f"{description}: response was not valid JSON"
                ) from e

        if _is_retriable_status(response.status_code) and attempt < retry.max_retries:
            attempt += 1
            time.sleep(retry.delay(attempt))
            continue

        raise OperationalError(
            f"{description}: {describe_error_response(response)}",
            response.status_code,
        )
