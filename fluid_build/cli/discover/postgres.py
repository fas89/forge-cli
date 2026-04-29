# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Postgres source discoverer.

Connects via DuckDB's ``postgres`` extension (no psycopg dependency in
this layer) and walks ``information_schema`` to enumerate every
``schema.table`` plus each column's name + data type + nullability.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, List
from urllib.parse import urlparse

from fluid_build.providers._sql_safety import quote_string_literal

from .registry import DiscoveredColumn, DiscoveredStream, Discoverer

LOG = logging.getLogger("fluid.cli.discover.postgres")


@dataclass
class PostgresDiscoverer(Discoverer):
    scheme: str = "postgres"

    def discover(self, uri: str) -> List[DiscoveredStream]:
        parts = _parse_postgres_uri(uri)
        return _introspect(parts)


def _parse_postgres_uri(uri: str) -> dict:
    p = urlparse(uri)
    if p.scheme not in ("postgres", "postgresql"):
        raise ValueError(f"PostgresDiscoverer expects postgres:// scheme, got {p.scheme}")
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 5432,
        "user": p.username or "",
        "password": p.password or "",
        "database": p.path.lstrip("/") or "postgres",
    }


def _libpq_escape(value: Any) -> str:
    """Escape one libpq DSN value. Strings with whitespace / special
    characters are wrapped in single quotes with internal quotes and
    backslashes doubled (libpq syntax). Plain alphanumerics pass through.
    """
    raw = str(value)
    s = raw.replace("\\", "\\\\").replace("'", "\\'")
    if " " in raw or "'" in raw or "\\" in raw:
        return f"'{s}'"
    return s


def _introspect(conn: dict) -> List[DiscoveredStream]:
    """Introspect a Postgres source and emit ``DiscoveredStream``s.

    The DSN is built with libpq-style escaping for each value, then the
    full DSN string is passed through ``quote_string_literal`` at the SQL
    boundary. Per-table column queries also pass schema / table through
    ``quote_string_literal`` so a malicious value returned by a tampered
    Postgres host's catalog can't break out of the SQL string literal.
    """
    import duckdb

    port_val = conn.get("port", 5432)
    if not str(port_val).isdigit():
        raise ValueError(f"postgres connection.port must be numeric, got {port_val!r}")

    dsn = (
        f"host={_libpq_escape(conn['host'])} "
        f"port={port_val} "
        f"user={_libpq_escape(conn['user'])} "
        f"password={_libpq_escape(conn['password'])} "
        f"dbname={_libpq_escape(conn['database'])}"
    )
    con = duckdb.connect(":memory:")
    streams: List[DiscoveredStream] = []
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH {quote_string_literal(dsn)} AS pg (TYPE postgres)")
        tables = con.execute(
            """
            SELECT table_schema, table_name
            FROM pg.information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
        ).fetchall()
        for schema, table in tables:
            quoted_schema = quote_string_literal(str(schema))
            quoted_table = quote_string_literal(str(table))
            cols = con.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM pg.information_schema.columns "
                f"WHERE table_schema = {quoted_schema} AND table_name = {quoted_table} "
                "ORDER BY ordinal_position"
            ).fetchall()
            streams.append(
                DiscoveredStream(
                    name=f"{schema}.{table}",
                    columns=[
                        DiscoveredColumn(
                            name=c[0],
                            type=str(c[1]),
                            nullable=str(c[2]).upper() == "YES",
                        )
                        for c in cols
                    ],
                    metadata={"schema": schema, "table": table},
                )
            )
    finally:
        con.close()
    return streams
