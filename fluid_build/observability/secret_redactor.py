# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Central logging redaction for secret-like values."""

from __future__ import annotations

import logging
import re
import traceback
from collections.abc import Mapping
from typing import Any

_REDACTED = "***REDACTED***"
_SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "auth_token",
    "authorization",
    "client_secret",
    "oauth_token",
    "password",
    "passphrase",
    "private_key",
    "secret",
    "session_token",
    "token",
)
_JWT_RE = re.compile(r"\b[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b")
_BEARER_RE = re.compile(r"(?i)\b(Bearer\s+)([^\s,;]+)")
_ASSIGNMENT_RE = re.compile(
    r"(?ix)"
    r"(?P<key>\b(?:[A-Za-z0-9_]*_)?(?:"
    r"api[_-]?key|authorization|aws_secret_access_key|client_secret|"
    r"oauth[_-]?token|password|private[_-]?key(?:_passphrase)?|secret|token"
    r")\b)"
    r"(?P<sep>\s*[:=]\s*)"
    r"(?P<quote>['\"]?)"
    r"(?P<value>.*?)(?P=quote)"
    r"(?=(?:[\s,;}\]]|$))"
)
_SENSITIVE_PLACEHOLDER_RE = re.compile(
    r"(?ix)\b(?:[A-Za-z0-9_]*_)?(?:"
    r"api[_-]?key|authorization|aws_secret_access_key|client_secret|"
    r"oauth[_-]?token|password|private[_-]?key(?:_passphrase)?|secret|token"
    r")\b\s*[:=]\s*%"
)


def redact_secret_text(text: str) -> str:
    """Redact secret-like substrings in plain text."""
    if not isinstance(text, str) or not text:
        return text

    redacted = _BEARER_RE.sub(r"\1" + _REDACTED, text)
    redacted = _JWT_RE.sub(_REDACTED, redacted)
    redacted = _ASSIGNMENT_RE.sub(
        lambda match: f"{match.group('key')}{match.group('sep')}{match.group('quote')}"
        f"{_REDACTED}{match.group('quote')}",
        redacted,
    )
    return redacted


def _is_sensitive_key(key: Any) -> bool:
    if not isinstance(key, str):
        return False
    lowered = key.lower()
    return any(part in lowered for part in _SENSITIVE_KEY_PARTS)


def redact_value(value: Any) -> Any:
    """Recursively redact secret-like values in logging payloads."""
    if isinstance(value, str):
        return redact_secret_text(value)
    if isinstance(value, Mapping):
        return {
            key: (_REDACTED if _is_sensitive_key(key) else redact_value(item))
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_value(item) for item in value)
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, set):
        return {redact_value(item) for item in value}
    return value


def _redact_template_args(args: Any) -> Any:
    if isinstance(args, tuple):
        return tuple(_redact_template_arg(arg) for arg in args)
    if isinstance(args, list):
        return [_redact_template_arg(arg) for arg in args]
    return _redact_template_arg(args)


def _redact_template_arg(arg: Any) -> Any:
    if isinstance(arg, (Mapping, list, tuple, set)):
        return redact_value(arg)
    return _REDACTED


class SecretRedactingFilter(logging.Filter):
    """Best-effort log filter that scrubs common credential leaks."""

    def filter(self, record: logging.LogRecord) -> bool:
        if hasattr(record, "args") and record.args:
            if isinstance(record.msg, str) and _SENSITIVE_PLACEHOLDER_RE.search(record.msg):
                record.args = _redact_template_args(record.args)
            else:
                record.args = redact_value(record.args)
        elif hasattr(record, "msg"):
            record.msg = redact_value(record.msg)
        if record.exc_info:
            record.exc_text = redact_secret_text(
                "".join(traceback.format_exception(*record.exc_info))
            )
        if record.stack_info:
            record.stack_info = redact_secret_text(record.stack_info)
        return True


def install_secret_redacting_filter(logger: logging.Logger) -> SecretRedactingFilter:
    """Attach one shared secret-redacting filter to a logger and its handlers."""
    for existing in logger.filters:
        if isinstance(existing, SecretRedactingFilter):
            secret_filter = existing
            break
    else:
        secret_filter = SecretRedactingFilter()
        logger.addFilter(secret_filter)

    for handler in logger.handlers:
        if not any(isinstance(existing, SecretRedactingFilter) for existing in handler.filters):
            handler.addFilter(secret_filter)

    return secret_filter
