"""
HTTP client options shared by the auth (OAuth, token exchange) and query clients.
"""

from __future__ import annotations

from typing import Dict, Optional, Union

import requests


class HttpOptions:
    """
    Transport settings applied to every HTTP request the driver makes.

    Either let the driver build a ``requests.Session`` from ``verify`` /
    ``proxies``, or pass your own ``session`` (e.g. with custom adapters, client
    certificates or auth hooks) and configure it yourself. Environment proxy
    variables (HTTPS_PROXY etc.) are honoured by default through requests.
    """

    DEFAULT_TIMEOUT_SECONDS = 30

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        verify: Optional[Union[bool, str]] = None,
        proxies: Optional[Dict[str, str]] = None,
        session: Optional[requests.Session] = None,
    ):
        """
        Args:
            timeout: Per-request timeout in seconds (default: 30)
            verify: TLS verification: True/False, or a path to a CA bundle
            proxies: Proxy mapping, e.g. {"https": "http://proxy:3128"}
            session: Caller-supplied Session to use instead of a new one.
                Mutually exclusive with ``verify`` and ``proxies``.

        Raises:
            ValueError: If timeout is not positive, or session is combined
                with verify/proxies
        """
        if timeout is None or timeout <= 0:
            raise ValueError("timeout must be > 0")
        if session is not None and (verify is not None or proxies is not None):
            raise ValueError(
                "Pass either session or verify/proxies, not both; "
                "configure verify/proxies on your own session"
            )

        self.timeout = timeout
        if session is None:
            session = requests.Session()
            if verify is not None:
                session.verify = verify
            if proxies:
                session.proxies.update(proxies)
        self.session = session
