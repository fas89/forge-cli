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

# fluid_build/providers/snowflake/util/config.py
"""Snowflake configuration utilities with secure credential resolution."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def resolve_account_and_warehouse(
    account: Optional[str] = None, warehouse: Optional[str] = None
) -> Tuple[str, str]:
    """
    Resolve Snowflake account and warehouse using unified credential resolver.

    Priority order:
    1. Explicit parameters
    2. Credential resolver chain (keyring, encrypted file, env vars, Vault)
    3. Default values

    Returns:
        Tuple of (account, warehouse)

    Raises:
        ValueError: If account cannot be determined
    """
    try:
        from ..credentials import SnowflakeCredentialAdapter

        adapter = SnowflakeCredentialAdapter(allow_prompt=False)
        resolver = adapter._get_resolver()

        # Resolve account
        if not account:
            account = resolver.get_credential(
                key="account", provider="snowflake", required=False, cli_value=None
            )

        # Resolve warehouse
        if not warehouse:
            warehouse = resolver.get_credential(
                key="warehouse", provider="snowflake", required=False, cli_value=None
            )
            if not warehouse:
                warehouse = "COMPUTE_WH"  # Default warehouse

    except Exception as e:
        logger.debug(f"Credential resolver not available, falling back to env vars: {e}")

        # Fallback to environment variables (backward compatibility)
        account = account or os.environ.get("SNOWFLAKE_ACCOUNT") or os.environ.get("SF_ACCOUNT")
        warehouse = (
            warehouse
            or os.environ.get("SNOWFLAKE_WAREHOUSE")
            or os.environ.get("SF_WAREHOUSE")
            or "COMPUTE_WH"
        )

    if not account:
        raise ValueError(
            "Snowflake account not specified. "
            "Provide via 'account' parameter, SNOWFLAKE_ACCOUNT environment variable, "
            "or store in keyring with: fluid auth set snowflake --account YOUR_ACCOUNT"
        )

    return account, warehouse


def get_connection_params(
    account: str,
    warehouse: str,
    database: Optional[str] = None,
    schema: Optional[str] = "PUBLIC",
    user: Optional[str] = None,
    **kwargs,
) -> dict:
    """
    Build connection parameters using unified credential resolver.

    This function now uses the SnowflakeCredentialAdapter which provides:
    - Secure credential resolution (keyring, .env, Vault, etc.)
    - Multiple authentication methods (password, key-pair, OAuth, SSO)
    - Backward compatibility with environment variables

    Args:
        account: Snowflake account identifier
        warehouse: Compute warehouse name
        database: Optional database name
        schema: Schema name (default: PUBLIC)
        user: Optional username (resolved via credential chain if not provided)
        **kwargs: Additional parameters and credential overrides

    Returns:
        Connection parameters dict for snowflake-connector-python
    """
    try:
        from fluid_build.credentials import CredentialConfig, get_snowflake_adapter

        # Create adapter with project root for .env file support
        config = CredentialConfig(
            project_root=kwargs.get("project_root") or Path.cwd(),
            environment=kwargs.get("environment", "dev"),
        )
        adapter = get_snowflake_adapter(config)

        # Build connection params using secure credential resolution
        params = adapter.get_connection_params(
            cli_value_account=account,
            cli_value_user=user,
            cli_value_warehouse=warehouse,
            cli_value_database=database,
            cli_value_schema=schema,
            **kwargs,
        )

        logger.info("✅ Snowflake credentials resolved securely")
        return params

    except ImportError as e:
        logger.warning(f"Credential resolver not available, using legacy method: {e}")

        # Fallback to legacy environment variable method (backward compatibility)
        return _get_connection_params_legacy(account, warehouse, database, schema, user, **kwargs)


def _get_connection_params_legacy(
    account: str,
    warehouse: str,
    database: Optional[str] = None,
    schema: Optional[str] = "PUBLIC",
    user: Optional[str] = None,
    **kwargs,
) -> dict:
    """
    Legacy connection params builder (backward compatibility).

    This is the original implementation that uses environment variables directly.
    Kept for backward compatibility if credential resolver is not available.
    """
    # Resolve user
    user = user or os.environ.get("SNOWFLAKE_USER") or os.environ.get("SF_USER")

    if not user:
        raise ValueError(
            "Snowflake user not specified. "
            "Provide via 'user' parameter or SNOWFLAKE_USER environment variable."
        )

    # Base parameters
    params = {
        "account": account,
        "user": user,
        "warehouse": warehouse,
    }

    if database:
        params["database"] = database
    if schema:
        params["schema"] = schema

    # Authentication method (priority order)
    if "password" in kwargs:
        params["password"] = kwargs["password"]
    elif "SNOWFLAKE_PASSWORD" in os.environ:
        params["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    elif "private_key" in kwargs:
        params["private_key"] = kwargs["private_key"]
    elif "token" in kwargs:
        params["token"] = kwargs["token"]
    elif "authenticator" in kwargs:
        params["authenticator"] = kwargs["authenticator"]
    elif os.environ.get("SNOWFLAKE_AUTHENTICATOR"):
        params["authenticator"] = os.environ["SNOWFLAKE_AUTHENTICATOR"]
    else:
        # Default to external browser for SSO
        params["authenticator"] = "externalbrowser"

    # Optional parameters
    for key in ["role", "application", "insecure_mode", "ocsp_response_cache_filename"]:
        if key in kwargs:
            params[key] = kwargs[key]

    return params
