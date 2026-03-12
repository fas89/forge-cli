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

# fluid_build/providers/local/util/__init__.py
"""
Utility modules for Local Provider.
"""

from .retry import (
    RetryableError,
    NonRetryableError,
    is_retryable_error,
    with_retry,
    retry,
    exponential_backoff,
    CircuitBreaker,
    CircuitBreakerOpenError,
)

from .logging import (
    redact_string,
    redact_dict,
    redact_list,
    format_event,
    safe_json_dumps,
    create_operation_logger,
    duration_ms,
    truncate_large_data,
)

__all__ = [
    # Retry utilities
    "RetryableError",
    "NonRetryableError",
    "is_retryable_error",
    "with_retry",
    "retry",
    "exponential_backoff",
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    # Logging utilities
    "redact_string",
    "redact_dict",
    "redact_list",
    "format_event",
    "safe_json_dumps",
    "create_operation_logger",
    "duration_ms",
    "truncate_large_data",
]
