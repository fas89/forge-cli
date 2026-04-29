# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""MySQL source discoverer (DuckDB mysql extension)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List
from urllib.parse import urlparse

from fluid_build.providers._sql_safety import quote_string_literal

from .registry import DiscoveredColumn, DiscoveredStream, Discoverer


@dataclass
class MySqlDiscoverer(Discoverer):
    scheme: str = "mysql"

    def discover(self, uri: str) -> List[DiscoveredStream]:
        p = urlparse(uri)
        if p.scheme not in ("mysql", "mariadb"):
            raise ValueError(f"MySqlDiscoverer expects mysql:// scheme, got {p.scheme}")
        return _introspect(
            host=p.hostname or "localhost",
            port=p.port or 3306,
            user=p.username or "",
            password=p.password or "",
            database=p.path.lstrip("/") or "",
        )


def _libpq_escape(value: Any) -> str:
    raw = str(value)
    s = raw.replace("\\", "\\\\").replace("'", "\\'")
    if " " in raw or "'" in raw or "\\" in raw:
        return f"'{s}'"
    return s


def _introspect(
    *, host: str, port: int, user: str, password: str, database: str
) -> List[DiscoveredStream]:
    """Introspect a MySQL/MariaDB source.

    DSN values are escaped per libpq-style rules then wrapped in
    ``quote_string_literal`` for the SQL boundary. Per-table column
    queries pass schema/table values through ``quote_string_literal``
    so attacker-controlled catalog data cannot break out of the SQL
    string literal.
    """
    import duckdb

    if not str(port).isdigit():
        raise ValueError(f"mysql connection.port must be numeric, got {port!r}")

    dsn = (
        f"host={_libpq_escape(host)} "
        f"port={port} "
        f"user={_libpq_escape(user)} "
        f"password={_libpq_escape(password)} "
        f"database={_libpq_escape(database)}"
    )
    con = duckdb.connect(":memory:")
    streams: List[DiscoveredStream] = []
    try:
        con.execute("INSTALL mysql; LOAD mysql;")
        con.execute(f"ATTACH {quote_string_literal(dsn)} AS mysql_db (TYPE mysql)")
        quoted_db = quote_string_literal(database)
        tables = con.execute(
            "SELECT table_schema, table_name "
            "FROM mysql_db.information_schema.tables "
            f"WHERE table_schema = {quoted_db} AND table_type = 'BASE TABLE' "
            "ORDER BY table_schema, table_name"
        ).fetchall()
        for schema, table in tables:
            quoted_schema = quote_string_literal(str(schema))
            quoted_table = quote_string_literal(str(table))
            cols = con.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM mysql_db.information_schema.columns "
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
