# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Shared rate-limiter instance.

Kept in its own module to avoid circular imports between api/main.py and routers
that import the limiter to decorate endpoints.
"""

from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
