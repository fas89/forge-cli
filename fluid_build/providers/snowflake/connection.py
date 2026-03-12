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

# fluid_build/provider/snowflake/connection.py
from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any, Dict, Optional

try:
    import snowflake.connector

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None

from .types import ProviderOptions

log = logging.getLogger("fluid.provider.snowflake")


class SnowflakeConnection:
    def __init__(self, opts: ProviderOptions = None, **kwargs):
        """
        Initialize Snowflake connection.

        Accepts either a ProviderOptions dataclass or keyword arguments
        matching ProviderOptions fields. This supports both:
            SnowflakeConnection(opts=provider_options)
            SnowflakeConnection(**connection_params_dict)
        """
        if opts is not None:
            self.opts = opts
        elif kwargs:
            # Build ProviderOptions from keyword args
            self.opts = ProviderOptions(
                account=kwargs.get("account", ""),
                user=kwargs.get("user", ""),
                password=kwargs.get("password"),
                private_key_path=kwargs.get("private_key_path"),
                private_key_passphrase=kwargs.get("private_key_passphrase"),
                role=kwargs.get("role"),
                warehouse=kwargs.get("warehouse"),
                database=kwargs.get("database"),
                schema=kwargs.get("schema"),
                oauth_token=kwargs.get("oauth_token"),
                authenticator=kwargs.get("authenticator"),
                session_params=kwargs.get("session_params"),
            )
        else:
            raise ValueError("SnowflakeConnection requires either opts or keyword arguments")
        self._conn = None

    def __enter__(self):
        self._conn = self._connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._conn is not None:
                self._conn.close()
        finally:
            self._conn = None

    def _connect(self):
        if not SNOWFLAKE_AVAILABLE:
            raise RuntimeError(
                "snowflake-connector-python not installed. "
                "Install with: pip install snowflake-connector-python"
            )

        kwargs: Dict[str, Any] = dict(
            account=self.opts.account,
            user=self.opts.user,
            role=self.opts.role,
            warehouse=self.opts.warehouse,
            database=self.opts.database,
            schema=self.opts.schema,
            session_parameters=self.opts.session_params or {"QUERY_TAG": "fluid-forge"},
        )

        if self.opts.oauth_token:
            kwargs["authenticator"] = "oauth"
            kwargs["token"] = self.opts.oauth_token
        elif self.opts.private_key_path:
            from cryptography.hazmat.primitives import serialization

            with open(self.opts.private_key_path, "rb") as f:
                pkey = serialization.load_pem_private_key(
                    f.read(),
                    password=(
                        self.opts.private_key_passphrase.encode("utf-8")
                        if self.opts.private_key_passphrase
                        else None
                    ),
                )
            pkb = pkey.private_bytes(
                serialization.Encoding.DER,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            kwargs["private_key"] = pkb
        else:
            kwargs["password"] = self.opts.password

        log.info(
            "Connecting to Snowflake account=%s role=%s wh=%s db=%s sch=%s",
            kwargs.get("account"),
            kwargs.get("role"),
            kwargs.get("warehouse"),
            kwargs.get("database"),
            kwargs.get("schema"),
        )
        return snowflake.connector.connect(**kwargs)

    def execute(self, sql: str, params: Optional[Iterable] = None, many: bool = False):
        log.debug("Executing SQL:\n%s", sql)
        with self._conn.cursor() as cur:
            if many and isinstance(params, list):
                cur.executemany(sql, params)
            elif params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return None

    def executescript(self, script: str):
        # Split on ; safely (Snowflake supports multi-statements when enabled; we do manual split).
        stmts = [s.strip() for s in script.split(";") if s.strip()]
        for s in stmts:
            self.execute(s)
