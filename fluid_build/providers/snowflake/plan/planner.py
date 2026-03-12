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

# fluid_build/providers/snowflake/plan/planner.py
"""
Snowflake Planning Engine - Generates execution plan from FLUID contract.

5-Phase Architecture:
1. Infrastructure: Databases, schemas, warehouses
2. IAM: Roles, grants, row-level security
3. Build: Stored procedures, UDFs, tasks
4. Expose: Tables, views, streams
5. Schedule: Task orchestration, pipes

Enhanced with governance:
- Tag extraction from contract (mirrors GCP labels)
- Policy tag support for column-level classification
- Metadata propagation to Snowflake objects
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from ..util.metadata import extract_snowflake_tags


def plan_actions(
    contract: Mapping[str, Any],
    account: str,
    warehouse: str,
    database: Optional[str],
    schema: str,
    logger=None,
) -> List[Dict[str, Any]]:
    """
    Generate ordered action list from FLUID contract.

    Phases ensure dependency ordering:
    - Infrastructure must exist before schemas
    - Schemas must exist before tables
    - Tables must exist before views/streams
    - IAM can run in parallel with build phase
    - Schedule runs after all objects exist
    """
    actions: List[Dict[str, Any]] = []

    # Phase 1: Infrastructure (databases, schemas, warehouses)
    actions.extend(_plan_infrastructure(contract, account, warehouse, database, schema, logger))

    # Phase 2: IAM (roles, grants, row-level security)
    actions.extend(_plan_iam(contract, account, database, schema, logger))

    # Phase 3: Build (stored procedures, UDFs, tasks)
    actions.extend(_plan_build(contract, account, database, schema, logger))

    # Phase 4: Expose (tables, views, streams)
    actions.extend(_plan_expose(contract, account, database, schema, logger))

    # Phase 5: Schedule (task orchestration, pipes)
    actions.extend(_plan_schedule(contract, account, database, schema, logger))

    return actions


def _plan_infrastructure(
    contract: Mapping[str, Any],
    account: str,
    warehouse: str,
    database: Optional[str],
    schema: str,
    logger=None,
) -> List[Dict[str, Any]]:
    """Phase 1: Create databases and schemas."""
    actions: List[Dict[str, Any]] = []

    # Extract binding information
    binding = contract.get("binding", {})
    location = binding.get("location", {})

    # Resolve database from multiple sources
    db_name = (
        location.get("database")
        or database
        or contract.get("metadata", {}).get("name", "").upper().replace("-", "_")
    )

    schema_name = location.get("schema") or schema

    # Ensure database exists
    if db_name:
        actions.append(
            {
                "id": f"database_{db_name}",
                "op": "sf.database.ensure",
                "phase": "infrastructure",
                "account": account,
                "database": db_name,
                "transient": False,
                "comment": f"Database for {contract.get('metadata', {}).get('name', 'FLUID contract')}",
            }
        )

    # Ensure schema exists
    if db_name and schema_name:
        actions.append(
            {
                "id": f"schema_{db_name}_{schema_name}",
                "op": "sf.schema.ensure",
                "phase": "infrastructure",
                "account": account,
                "database": db_name,
                "schema": schema_name,
                "transient": False,
                "comment": f"Schema for {contract.get('metadata', {}).get('name', 'FLUID contract')}",
            }
        )

    return actions


def _plan_iam(
    contract: Mapping[str, Any],
    account: str,
    database: Optional[str],
    schema: str,
    logger=None,
) -> List[Dict[str, Any]]:
    """Phase 2: Configure roles and grants."""
    actions: List[Dict[str, Any]] = []

    # Extract IAM configuration from contract
    security = contract.get("security", {})
    access_control = security.get("access_control", {})

    # Grant privileges to roles
    grants = access_control.get("grants", [])
    for grant in grants:
        role = grant.get("role")
        privilege = grant.get("privilege")
        object_type = grant.get("object_type")
        object_name = grant.get("object_name")

        if role and privilege:
            actions.append(
                {
                    "id": f"grant_{role}_{privilege}_{object_type}_{object_name}",
                    "op": "sf.grant.privilege",
                    "phase": "iam",
                    "account": account,
                    "role": role,
                    "privilege": privilege,
                    "object_type": object_type or "TABLE",
                    "object_name": object_name,
                    "database": database,
                }
            )

    # Row-level security policies
    row_level_security = security.get("row_level_security", [])
    for policy in row_level_security:
        table = policy.get("table")
        role = policy.get("role")
        condition = policy.get("condition")

        if table and role and condition:
            actions.append(
                {
                    "id": f"rls_{table}_{role}",
                    "op": "sf.sql.execute",
                    "phase": "iam",
                    "account": account,
                    "database": database,
                    "sql": f"CREATE OR REPLACE ROW ACCESS POLICY {table}_rls AS (val VARCHAR) RETURNS BOOLEAN -> CASE WHEN CURRENT_ROLE() = '{role}' THEN {condition} ELSE FALSE END",
                    "comment": f"Row-level security for {table}",
                }
            )

    return actions


def _plan_build(
    contract: Mapping[str, Any],
    account: str,
    database: Optional[str],
    schema: str,
    logger=None,
) -> List[Dict[str, Any]]:
    """Phase 3: Create stored procedures, UDFs, tasks."""
    actions: List[Dict[str, Any]] = []

    # Extract build configuration
    build = contract.get("build", {})

    # Stored procedures
    procedures = build.get("procedures", [])
    for proc in procedures:
        name = proc.get("name")
        language = proc.get("language", "SQL")
        body = proc.get("body")
        params = proc.get("parameters", [])

        if name and body:
            actions.append(
                {
                    "id": f"procedure_{name}",
                    "op": "sf.procedure.ensure",
                    "phase": "build",
                    "account": account,
                    "database": database,
                    "schema": schema,
                    "name": name,
                    "language": language,
                    "parameters": params,
                    "body": body,
                }
            )

    # User-defined functions (UDFs)
    udfs = build.get("udfs", [])
    for udf in udfs:
        name = udf.get("name")
        language = udf.get("language", "SQL")
        return_type = udf.get("return_type", "VARCHAR")
        body = udf.get("body")
        params = udf.get("parameters", [])

        if name and body:
            actions.append(
                {
                    "id": f"udf_{name}",
                    "op": "sf.udf.ensure",
                    "phase": "build",
                    "account": account,
                    "database": database,
                    "schema": schema,
                    "name": name,
                    "language": language,
                    "return_type": return_type,
                    "parameters": params,
                    "body": body,
                }
            )

    # Embedded SQL scripts
    sql_scripts = build.get("sql", [])
    for i, script in enumerate(sql_scripts):
        if isinstance(script, str):
            sql_text = script
            script_id = f"sql_{i}"
        elif isinstance(script, dict):
            sql_text = script.get("sql")
            script_id = script.get("id", f"sql_{i}")
        else:
            continue

        if sql_text:
            actions.append(
                {
                    "id": script_id,
                    "op": "sf.sql.execute",
                    "phase": "build",
                    "account": account,
                    "database": database,
                    "sql": sql_text,
                }
            )

    return actions


def _plan_expose(
    contract: Mapping[str, Any],
    account: str,
    database: Optional[str],
    schema: str,
    logger=None,
) -> List[Dict[str, Any]]:
    """Phase 4: Create tables, views, streams with governance metadata."""
    actions: List[Dict[str, Any]] = []

    # Process exposes array (0.5.7/0.7.1 pattern)
    for expose in contract.get("exposes", []):
        expose_id = expose.get("exposeId", expose.get("id"))

        # Extract tags from contract + expose (8 sources, mirrors GCP)
        table_tags = extract_snowflake_tags(contract, expose)

        # Get binding information
        binding = expose.get("binding", {})
        location = binding.get("location", {})
        format_type = binding.get("format", "snowflake_table")

        # Resolve names
        db_name = location.get("database") or database
        schema_name = location.get("schema") or schema
        table_name = location.get("table") or expose_id

        # Tables from contract schema
        contract_schema = expose.get("contract", {})
        fields = contract_schema.get("schema", [])

        if table_name and fields and format_type == "snowflake_table":
            # Convert FLUID fields to Snowflake columns with tags
            columns = []
            for field in fields:
                col_name = field.get("name")
                col_type = _map_fluid_type_to_snowflake(field.get("type", "string"))
                nullable = field.get("nullable", True)
                description = field.get("description")

                col_def = {
                    "name": col_name,
                    "type": col_type,
                    "nullable": nullable,
                    "labels": field.get("labels", {}),  # Pass labels for tag extraction
                }
                if description:
                    col_def["comment"] = description

                columns.append(col_def)

            # Create table action with tags
            actions.append(
                {
                    "id": f"table_{db_name}_{schema_name}_{table_name}",
                    "op": "sf.table.ensure",
                    "phase": "expose",
                    "account": account,
                    "database": db_name,
                    "schema": schema_name,
                    "table": table_name,
                    "columns": columns,
                    "cluster_by": contract_schema.get("cluster_by", []),
                    "comment": expose.get("description") or expose.get("title"),
                    "tags": table_tags,  # Table-level tags
                    "contract": contract,  # Full contract for metadata
                }
            )

    # Views
    views = contract.get("views", [])
    for view in views:
        view_name = view.get("name")
        query = view.get("query")
        materialized = view.get("materialized", False)

        if view_name and query:
            op = "sf.view.materialized.ensure" if materialized else "sf.view.ensure"
            actions.append(
                {
                    "id": f"view_{view_name}",
                    "op": op,
                    "phase": "expose",
                    "account": account,
                    "database": db_name,
                    "schema": schema_name,
                    "name": view_name,
                    "query": query,
                    "secure": view.get("secure", False),
                }
            )

    # Streams (for CDC)
    streams = contract.get("streams", [])
    for stream in streams:
        stream_name = stream.get("name")
        source_table = stream.get("source_table")

        if stream_name and source_table:
            actions.append(
                {
                    "id": f"stream_{stream_name}",
                    "op": "sf.stream.ensure",
                    "phase": "expose",
                    "account": account,
                    "database": db_name,
                    "schema": schema_name,
                    "name": stream_name,
                    "source_table": source_table,
                    "append_only": stream.get("append_only", False),
                }
            )

    return actions


def _plan_schedule(
    contract: Mapping[str, Any],
    account: str,
    database: Optional[str],
    schema: str,
    logger=None,
) -> List[Dict[str, Any]]:
    """Phase 5: Configure task orchestration."""
    actions: List[Dict[str, Any]] = []

    # Extract orchestration configuration
    orchestration = contract.get("orchestration", {})

    # Tasks
    tasks = orchestration.get("tasks", [])
    for task in tasks:
        task_name = task.get("name")
        schedule = task.get("schedule")
        sql = task.get("sql")

        if task_name and sql:
            actions.append(
                {
                    "id": f"task_{task_name}",
                    "op": "sf.task.ensure",
                    "phase": "schedule",
                    "account": account,
                    "database": database,
                    "schema": schema,
                    "name": task_name,
                    "schedule": schedule,
                    "sql": sql,
                    "warehouse": task.get("warehouse"),
                    "after": task.get("after", []),  # Task dependencies
                }
            )

            # Auto-resume task if requested
            if task.get("enabled", True):
                actions.append(
                    {
                        "id": f"task_resume_{task_name}",
                        "op": "sf.task.resume",
                        "phase": "schedule",
                        "account": account,
                        "database": database,
                        "schema": schema,
                        "name": task_name,
                    }
                )

    return actions


def _map_fluid_type_to_snowflake(fluid_type: str) -> str:
    """
    Map FLUID type to Snowflake data type.

    FLUID Types → Snowflake Types:
    - string → VARCHAR
    - integer → NUMBER(38,0)
    - long → NUMBER(38,0)
    - float → FLOAT
    - double → DOUBLE
    - decimal → NUMBER(38,10)
    - boolean → BOOLEAN
    - date → DATE
    - timestamp → TIMESTAMP_NTZ
    - binary → BINARY
    - array → ARRAY
    - object → OBJECT
    """
    type_map = {
        "string": "VARCHAR",
        "integer": "NUMBER(38,0)",
        "int": "NUMBER(38,0)",
        "long": "NUMBER(38,0)",
        "bigint": "NUMBER(38,0)",
        "float": "FLOAT",
        "double": "DOUBLE",
        "decimal": "NUMBER(38,10)",
        "numeric": "NUMBER(38,10)",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP_NTZ",
        "datetime": "TIMESTAMP_NTZ",
        "timestamp_ntz": "TIMESTAMP_NTZ",
        "timestamp_tz": "TIMESTAMP_TZ",
        "timestamp_ltz": "TIMESTAMP_LTZ",
        "time": "TIME",
        "binary": "BINARY",
        "array": "ARRAY",
        "object": "OBJECT",
        "variant": "VARIANT",
        "geography": "GEOGRAPHY",
        "geometry": "GEOMETRY",
    }

    return type_map.get(fluid_type.lower(), "VARCHAR")
