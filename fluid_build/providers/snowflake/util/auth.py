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

# fluid_build/providers/snowflake/util/auth.py
"""Snowflake authentication and environment reporting."""
from __future__ import annotations

import os
from typing import Any, Dict, Optional


def get_auth_report(
    account: str,
    warehouse: str,
    database: Optional[str] = None
) -> Dict[str, Any]:
    """
    Generate authentication and environment diagnostics.
    
    Useful for troubleshooting connection issues and verifying
    configuration before deployment.
    """
    report = {
        "provider": "snowflake",
        "status": "success",
        "account": account,
        "warehouse": warehouse,
        "database": database,
        "environment_variables": {}
    }
    
    # Check environment variables
    env_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_AUTHENTICATOR",
        "SF_ACCOUNT",
        "SF_USER",
        "SF_WAREHOUSE"
    ]
    
    for var in env_vars:
        value = os.environ.get(var)
        if value:
            # Redact sensitive values
            if "PASSWORD" in var or "TOKEN" in var or "KEY" in var:
                report["environment_variables"][var] = "***REDACTED***"
            else:
                report["environment_variables"][var] = value
    
    # Check for password in environment (redacted)
    if os.environ.get("SNOWFLAKE_PASSWORD"):
        report["environment_variables"]["SNOWFLAKE_PASSWORD"] = "***REDACTED***"
    
    # Attempt to query current user (requires connection)
    try:
        current_user, current_role = _get_current_identity(account, warehouse, database)
        report["current_user"] = current_user
        report["current_role"] = current_role
        report["connection_test"] = "success"
    except Exception as e:
        report["connection_test"] = "failed"
        report["connection_error"] = str(e)
    
    return report


def _get_current_identity(
    account: str,
    warehouse: str,
    database: Optional[str]
) -> tuple[str, str]:
    """
    Query Snowflake for current user and role.
    
    Requires active connection.
    """
    try:
        # Import connection utilities
        from ..connection import SnowflakeConnection
        from .config import get_connection_params
        
        # Build connection params
        params = get_connection_params(account, warehouse, database)
        
        # Query current identity
        with SnowflakeConnection(**params) as conn:
            user_result = conn.execute("SELECT CURRENT_USER()")
            current_user = user_result[0][0] if user_result else "unknown"
            
            role_result = conn.execute("SELECT CURRENT_ROLE()")
            current_role = role_result[0][0] if role_result else "unknown"
            
            return current_user, current_role
            
    except Exception:
        # Connection not available or failed
        return "unknown", "unknown"
