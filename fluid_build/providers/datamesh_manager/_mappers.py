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

"""Pure FLUID → Data Mesh Manager payload helpers.

Extracted from :mod:`fluid_build.providers.datamesh_manager.datamesh_manager`
to keep the provider class focused on orchestration + HTTP. Everything in
this module is a pure function — no HTTP, no logging, no class state.

The orchestrator's static methods delegate here so the test surface
(patches on ``DataMeshManagerProvider._extract_provider`` etc.) stays intact.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from fluid_build.providers.base import ProviderError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATA_PRODUCT_SPEC_DPS = "0.0.1"
DATA_PRODUCT_SPEC_ODPS = "odps"

STATUS_MAP: Dict[str, str] = {
    "draft": "draft",
    "development": "draft",
    "active": "active",
    "production": "active",
    "deprecated": "deprecated",
    "retired": "retired",
}

PROVIDER_TYPE_MAP: Dict[str, str] = {
    "gcp": "BigQuery",
    "bigquery": "BigQuery",
    "snowflake": "Snowflake",
    "databricks": "Databricks",
    "aws": "S3",
    "redshift": "Redshift",
    "kafka": "Kafka",
    "s3": "S3",
    "azure": "Azure",
    "postgres": "Postgres",
    "mysql": "MySQL",
    "local": "Local",
}


# ---------------------------------------------------------------------------
# ID & team helpers
# ---------------------------------------------------------------------------


def extract_id(fluid: Mapping[str, Any]) -> str:
    """Resolve a FLUID contract's id to a URL-safe path segment."""
    for path in (
        ("id",),
        ("contract", "id"),
        ("metadata", "id"),
        ("metadata", "name"),
        ("name",),
    ):
        node: Any = fluid
        for key in path:
            if isinstance(node, dict):
                node = node.get(key)
            else:
                node = None
                break
        if node and isinstance(node, str):
            return node.strip().lower().replace(" ", "-").replace("/", "-")
    raise ProviderError(
        "FLUID contract is missing a product id.  "
        "Set 'id', 'metadata.id', or 'metadata.name'."
    )


def derive_team_id(fluid: Mapping[str, Any]) -> str:
    owner = fluid.get("owner", fluid.get("metadata", {}).get("owner", {}))
    if isinstance(owner, dict):
        for key in ("team", "name", "id"):
            val = owner.get(key)
            if val and isinstance(val, str):
                return val.strip().lower().replace(" ", "-")
    return "default-team"


# ---------------------------------------------------------------------------
# Provider / location / server extraction
# ---------------------------------------------------------------------------


def extract_provider(section: Mapping[str, Any]) -> str:
    """Extract provider/platform name from an expose or expect block.

    Supports both legacy (``provider: gcp``) and FLUID 0.7.1
    (``binding.platform: gcp``) patterns.
    """
    binding = section.get("binding", {})
    if isinstance(binding, dict):
        platform = binding.get("platform", "")
        if platform:
            return str(platform)
    return str(section.get("provider", ""))


def resolve_location(section: Mapping[str, Any], provider: str) -> str:
    """Build a human-readable location string from provider config.

    Supports both legacy flat keys (``section.gcp``, ``section.snowflake``)
    and FLUID 0.7.1 ``binding.location`` pattern.
    """
    provider_lower = provider.lower() if provider else ""
    parts: List[str] = []

    binding = section.get("binding", {})
    if isinstance(binding, dict):
        loc = binding.get("location", {})
        if isinstance(loc, dict) and loc:
            if provider_lower in ("gcp", "bigquery"):
                for key in ("project", "dataset", "table"):
                    if key in loc:
                        parts.append(str(loc[key]))
                if parts:
                    return ".".join(parts)

            elif provider_lower == "snowflake":
                for key in ("database", "schema", "table"):
                    if key in loc:
                        parts.append(str(loc[key]))
                if parts:
                    return ".".join(parts)

            elif provider_lower in ("aws", "s3"):
                bucket = loc.get("bucket", "")
                path_val = loc.get("path", loc.get("prefix", loc.get("key", "")))
                if bucket:
                    result = f"s3://{bucket}"
                    if path_val:
                        result += "/{}".format(str(path_val).strip("/"))
                    return result
                for key in ("database", "table"):
                    if key in loc:
                        parts.append(str(loc[key]))
                if parts:
                    return ".".join(parts)

            elif provider_lower == "redshift":
                for key in ("database", "schema", "table"):
                    if key in loc:
                        parts.append(str(loc[key]))
                if parts:
                    return ".".join(parts)

            elif provider_lower == "kafka":
                topic = loc.get("topic", "")
                if topic:
                    return str(topic)

            if not parts:
                generic_parts = [
                    str(v)
                    for k, v in loc.items()
                    if k not in ("region",) and v and not str(v).startswith("{{")
                ]
                if generic_parts:
                    return ".".join(generic_parts)

    if provider_lower in ("gcp", "bigquery"):
        cfg = section.get("gcp", section.get("bigquery", {}))
        if isinstance(cfg, dict):
            for key in ("project", "dataset", "table"):
                if key in cfg:
                    parts.append(str(cfg[key]))

    elif provider_lower == "snowflake":
        cfg = section.get("snowflake", {})
        if isinstance(cfg, dict):
            for key in ("database", "schema", "table"):
                if key in cfg:
                    parts.append(str(cfg[key]))

    elif provider_lower in ("aws", "s3"):
        cfg = section.get("aws", section.get("s3", {}))
        if isinstance(cfg, dict):
            bucket = cfg.get("bucket", "")
            prefix = cfg.get("prefix", cfg.get("key", ""))
            if bucket:
                loc_str = f"s3://{bucket}"
                if prefix:
                    loc_str += f"/{prefix}"
                return loc_str

    elif provider_lower == "redshift":
        cfg = section.get("redshift", {})
        if isinstance(cfg, dict):
            for key in ("database", "schema", "table"):
                if key in cfg:
                    parts.append(str(cfg[key]))

    elif provider_lower == "kafka":
        cfg = section.get("kafka", {})
        if isinstance(cfg, dict):
            topic = cfg.get("topic", "")
            if topic:
                return str(topic)

    if not parts:
        conn = section.get("location") or section.get("connection", "")
        if isinstance(conn, dict):
            return str(conn.get("uri", conn.get("endpoint", "")))
        return str(conn) if conn else ""

    return ".".join(parts)


def build_server_object(section: Mapping[str, Any], provider: str) -> Dict[str, Any]:
    """Build a structured ``server`` object for an output port.

    The DPS schema expects keys like ``account``, ``database``,
    ``schema``, ``table``, ``topic``, ``location`` etc. inside a
    server object — NOT a flat location string.
    """
    server: Dict[str, Any] = {}
    provider_lower = provider.lower() if provider else ""

    binding = section.get("binding", {})
    if isinstance(binding, dict):
        loc = binding.get("location", {})
        if isinstance(loc, dict) and loc:
            if provider_lower in ("gcp", "bigquery"):
                if loc.get("project"):
                    server["account"] = str(loc["project"])
                if loc.get("dataset"):
                    server["database"] = str(loc["dataset"])
                if loc.get("table"):
                    server["table"] = str(loc["table"])
                return server

            if provider_lower == "snowflake":
                for key in ("account", "database", "schema", "table"):
                    if loc.get(key):
                        server[key] = str(loc[key])
                return server

            if provider_lower in ("aws", "s3"):
                bucket = loc.get("bucket", "")
                path_val = loc.get("path", loc.get("prefix", loc.get("key", "")))
                if bucket:
                    loc_str = f"s3://{bucket}"
                    if path_val:
                        loc_str += "/{}".format(str(path_val).strip("/"))
                    server["location"] = loc_str
                fmt = binding.get("format")
                if fmt:
                    server["format"] = str(fmt)
                return server

            if provider_lower == "redshift":
                for key in ("database", "schema", "table"):
                    if loc.get(key):
                        server[key] = str(loc[key])
                return server

            if provider_lower == "kafka":
                if loc.get("topic"):
                    server["topic"] = str(loc["topic"])
                return server

            for k, v in loc.items():
                if v and not str(v).startswith("{{") and k != "region":
                    server[k] = v
            return server

    cfg: Mapping[str, Any] = {}
    if provider_lower in ("gcp", "bigquery"):
        cfg = section.get("gcp", section.get("bigquery", {}))
        if isinstance(cfg, dict):
            if cfg.get("project"):
                server["account"] = str(cfg["project"])
            if cfg.get("dataset"):
                server["database"] = str(cfg["dataset"])
            if cfg.get("table"):
                server["table"] = str(cfg["table"])

    elif provider_lower == "snowflake":
        cfg = section.get("snowflake", {})
        if isinstance(cfg, dict):
            for key in ("account", "database", "schema", "table"):
                if cfg.get(key):
                    server[key] = str(cfg[key])

    elif provider_lower in ("aws", "s3"):
        cfg = section.get("aws", section.get("s3", {}))
        if isinstance(cfg, dict):
            bucket = cfg.get("bucket", "")
            prefix = cfg.get("prefix", cfg.get("key", ""))
            if bucket:
                loc_str = f"s3://{bucket}"
                if prefix:
                    loc_str += f"/{prefix}"
                server["location"] = loc_str

    elif provider_lower == "redshift":
        cfg = section.get("redshift", {})
        if isinstance(cfg, dict):
            for key in ("database", "schema", "table"):
                if cfg.get(key):
                    server[key] = str(cfg[key])

    elif provider_lower == "kafka":
        cfg = section.get("kafka", {})
        if isinstance(cfg, dict):
            if cfg.get("topic"):
                server["topic"] = str(cfg["topic"])

    if not server:
        conn = section.get("location") or section.get("connection", "")
        if isinstance(conn, dict):
            uri = conn.get("uri", conn.get("endpoint", ""))
            if uri:
                server["location"] = str(uri)
        elif conn:
            server["location"] = str(conn)

    return server


# ---------------------------------------------------------------------------
# Links / custom / type mappings
# ---------------------------------------------------------------------------


def extract_links(fluid: Mapping[str, Any]) -> Dict[str, str]:
    links: Dict[str, str] = {}
    meta = fluid.get("metadata", {})
    if isinstance(meta, dict):
        for key in ("documentation", "repository", "catalog", "dataProduct"):
            val = meta.get(key)
            if val:
                links[key] = str(val)
    top = fluid.get("links", {})
    if isinstance(top, dict):
        links.update({k: str(v) for k, v in top.items()})
    return links


def extract_custom(fluid: Mapping[str, Any]) -> Dict[str, Any]:
    custom: Dict[str, Any] = {}
    meta = fluid.get("metadata", {})
    if isinstance(meta, dict):
        for key in ("domain", "subdomain", "environment", "version", "layer", "sla"):
            val = meta.get(key)
            if val is not None:
                custom[key] = val
    explicit = fluid.get("custom", {})
    if not isinstance(explicit, dict):
        explicit = meta.get("custom", {}) if isinstance(meta, dict) else {}
    if isinstance(explicit, dict):
        custom.update(explicit)
    return custom


def odcs_logical_type(fluid_type: str) -> str:
    """Map FLUID/SQL types to ODCS logical types."""
    t = fluid_type.strip().lower()
    mapping = {
        "string": "string",
        "varchar": "string",
        "text": "string",
        "char": "string",
        "integer": "integer",
        "int": "integer",
        "int64": "integer",
        "bigint": "integer",
        "smallint": "integer",
        "float": "number",
        "float64": "number",
        "double": "number",
        "decimal": "number",
        "numeric": "number",
        "boolean": "boolean",
        "bool": "boolean",
        "date": "date",
        "datetime": "timestamp",
        "timestamp": "timestamp",
        "timestamp_ntz": "timestamp",
        "time": "string",
        "json": "object",
        "struct": "object",
        "array": "array",
        "binary": "binary",
        "bytes": "binary",
    }
    return mapping.get(t, "string")


def is_odps_spec(value: Optional[str]) -> bool:
    spec = str(value or "").strip().lower()
    return spec in {"odps", "opds"}


def is_odps_payload(payload: Mapping[str, Any]) -> bool:
    return bool(
        isinstance(payload, Mapping)
        and "apiVersion" in payload
        and str(payload.get("kind", "")).lower() == "dataproduct"
        and "info" not in payload
    )


def resolve_data_product_specification(
    value: Optional[str],
    *,
    provider_hint: Optional[str] = None,
    fluid: Optional[Mapping[str, Any]] = None,
) -> str:
    """Resolve outgoing dataProductSpecification.

    Resolution order:
    1) explicit ``value`` (e.g. ``data_product_specification="0.0.1"``)
    2) explicit ``provider_hint`` — ``dps`` → DPS, ``odps``/``opds`` → ODPS
    3) Bitol DataProduct convention: ``fluid["kind"] == "DataProduct"`` → ODPS
       (kept for documentation; same result as the default)
    4) **default: ODPS.** Entropy Data accepts the Bitol shape; the legacy
       DPS 0.0.1 shape is rejected with HTTP 400 by current DMM releases,
       so it's now opt-in via explicit value or ``provider_hint="dps"``.
    """
    if value:
        return str(value).strip()

    hint = str(provider_hint or "").strip().lower()
    if hint == "dps":
        return DATA_PRODUCT_SPEC_DPS
    if hint in {"odps", "opds"}:
        return DATA_PRODUCT_SPEC_ODPS

    if isinstance(fluid, Mapping) and str(fluid.get("kind", "")).lower() == "dataproduct":
        return DATA_PRODUCT_SPEC_ODPS

    return DATA_PRODUCT_SPEC_ODPS


# ---------------------------------------------------------------------------
# Port mapping
# ---------------------------------------------------------------------------


def _new_port_id() -> str:
    import uuid

    return str(uuid.uuid4())


def map_input_ports(fluid: Mapping[str, Any]) -> List[Dict[str, Any]]:
    ports: List[Dict[str, Any]] = []
    for expect in fluid.get("expects", []):
        port: Dict[str, Any] = {
            "id": expect.get("id", _new_port_id()),
            "name": expect.get("name") or expect.get("id", "input"),
            "description": expect.get("description", ""),
        }
        provider = extract_provider(expect)
        if provider:
            port["type"] = PROVIDER_TYPE_MAP.get(provider.lower(), provider.title())

        source_system = expect.get("source_system") or expect.get("sourceSystem")
        if source_system:
            port["sourceSystemId"] = source_system

        location = resolve_location(expect, provider)
        if location:
            port["location"] = location

        port_tags = list(expect.get("tags", []))
        if provider and provider not in port_tags:
            port_tags.insert(0, provider)
        if port_tags:
            port["tags"] = port_tags

        ports.append(port)
    return ports


def map_output_ports(
    fluid: Mapping[str, Any], product_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    ports: List[Dict[str, Any]] = []
    for expose in fluid.get("exposes", []):
        expose_id = expose.get("id", expose.get("exposeId", _new_port_id()))
        port: Dict[str, Any] = {
            "id": expose_id,
            "name": expose.get("name") or expose.get("title") or expose_id,
            "description": expose.get("description", ""),
        }

        lifecycle = expose.get("lifecycle", {})
        port["status"] = STATUS_MAP.get(
            str(lifecycle.get("state", "active")).lower(), "active"
        )

        if product_id:
            port["dataContractId"] = f"{product_id}.{expose_id}"

        provider = extract_provider(expose)
        if provider:
            port["type"] = PROVIDER_TYPE_MAP.get(provider.lower(), provider.title())

        server = build_server_object(expose, provider)
        if server:
            port["server"] = server

        port_links = expose.get("links", {})
        if isinstance(port_links, dict) and port_links:
            port["links"] = port_links

        schema = expose.get("schema", expose.get("contract", {}).get("schema", {}))
        fields = schema.get("fields", []) if isinstance(schema, dict) else []
        if isinstance(fields, list):
            port["containsPii"] = any(
                "pii" in str(f.get("classification", "")).lower()
                or f.get("pii", False)
                or "pii" in str(f.get("tags", "")).lower()
                for f in fields
            )
        else:
            port["containsPii"] = False

        port_tags = list(expose.get("tags", []))
        if provider and provider not in port_tags:
            port_tags.insert(0, provider)
        if port_tags:
            port["tags"] = port_tags

        custom = expose.get("custom", {})
        if isinstance(custom, dict) and custom:
            port["custom"] = custom

        ports.append(port)
    return ports


# ---------------------------------------------------------------------------
# DataProduct payload builders
# ---------------------------------------------------------------------------


def to_data_product(
    fluid: Mapping[str, Any],
    *,
    data_product_specification: Optional[str] = None,
) -> Dict[str, Any]:
    """Map a FLUID contract to the Entropy Data DataProduct shape (DPS 0.0.1)."""
    if is_odps_spec(data_product_specification):
        return to_data_product_odps(fluid)

    meta = fluid.get("metadata", {})
    product_id = extract_id(fluid)
    status = STATUS_MAP.get(str(meta.get("status", "draft")).lower(), "draft")

    info: Dict[str, Any] = {
        "title": meta.get("name") or fluid.get("name") or product_id,
        "owner": derive_team_id(fluid),
        "description": meta.get("description") or fluid.get("description", ""),
        "status": status,
    }

    if meta.get("archetype"):
        info["archetype"] = meta["archetype"]
    elif fluid.get("kind"):
        kind_lower = str(fluid["kind"]).lower()
        if kind_lower == "dataproduct":
            layer = str(meta.get("layer", "")).lower()
            if layer in ("bronze", "raw"):
                info["archetype"] = "source-aligned"
            elif layer in ("gold", "aggregate"):
                info["archetype"] = "aggregate"
            elif layer in ("silver", "curated"):
                info["archetype"] = "consumer-aligned"
    if meta.get("maturity"):
        info["maturity"] = meta["maturity"]

    dp: Dict[str, Any] = {
        "dataProductSpecification": data_product_specification or DATA_PRODUCT_SPEC_DPS,
        "id": product_id,
        "info": info,
    }

    input_ports = map_input_ports(fluid)
    if input_ports:
        dp["inputPorts"] = input_ports

    output_ports = map_output_ports(fluid, product_id=product_id)
    if output_ports:
        dp["outputPorts"] = output_ports

    links = extract_links(fluid)
    if links:
        dp["links"] = links

    all_tags: List[str] = []
    top_tags = fluid.get("tags", [])
    if isinstance(top_tags, list):
        all_tags.extend(top_tags)
    meta_tags = meta.get("tags", [])
    if isinstance(meta_tags, list):
        for t in meta_tags:
            if t not in all_tags:
                all_tags.append(t)
    if all_tags:
        dp["tags"] = all_tags

    custom = extract_custom(fluid)
    if custom:
        dp["custom"] = custom

    return dp


def to_data_product_odps(fluid: Mapping[str, Any]) -> Dict[str, Any]:
    """Map FLUID contract to ODPS data product shape (Bitol)."""
    try:
        from fluid_build.providers.odps_standard import OdpsStandardProvider
    except ImportError as exc:
        raise ProviderError(
            "OdpsStandardProvider is required for ODPS data product publish.\n"
            "Ensure fluid_build.providers.odps_standard is installed."
        ) from exc

    odps_provider = OdpsStandardProvider()
    odps_payload = odps_provider.render(normalize_fluid_for_odps_standard(fluid))
    odps_payload["id"] = extract_id(fluid)
    odps_payload.setdefault("kind", "DataProduct")
    return odps_payload


def normalize_fluid_for_odps_standard(fluid: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize FLUID structure for ODPS-Bitol converter compatibility."""
    normalized: Dict[str, Any] = dict(fluid)

    exposes = fluid.get("exposes", [])
    normalized_exposes: List[Dict[str, Any]] = []
    if isinstance(exposes, list):
        for expose in exposes:
            if not isinstance(expose, Mapping):
                continue
            expose_dict = dict(expose)
            if not expose_dict.get("id") and expose_dict.get("exposeId"):
                expose_dict["id"] = expose_dict["exposeId"]
            normalized_exposes.append(expose_dict)
    normalized["exposes"] = normalized_exposes

    return normalized


# ---------------------------------------------------------------------------
# DataContract payload builders
# ---------------------------------------------------------------------------


def build_data_contract_odcs(
    fluid: Mapping[str, Any], product_id: str
) -> Dict[str, Any]:
    """Build an Open Data Contract Standard v3.1.0 payload.

    Reference: https://bitol-io.github.io/open-data-contract-standard/
    """
    meta = fluid.get("metadata", {})
    contract_id = f"{product_id}-contract"

    dc: Dict[str, Any] = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": contract_id,
        "name": meta.get("name") or fluid.get("name") or product_id,
        "version": meta.get("version", "1.0.0"),
        "status": STATUS_MAP.get(str(meta.get("status", "active")).lower(), "active"),
        "dataProduct": product_id,
        "team": {
            "name": derive_team_id(fluid),
        },
    }

    domain = fluid.get("domain") or meta.get("domain")
    if domain:
        dc["domain"] = str(domain).lower().replace(" ", "-")

    desc_text = meta.get("description") or fluid.get("description", "")
    if desc_text:
        dc["description"] = {"purpose": str(desc_text).strip()}

    schema_array: List[Dict[str, Any]] = []
    servers: List[Dict[str, Any]] = []

    for expose in fluid.get("exposes", []):
        model_id = expose.get("id", expose.get("exposeId", "default"))

        raw_schema = expose.get("schema", {})
        if not raw_schema:
            contract_block = expose.get("contract", {})
            if isinstance(contract_block, dict):
                raw_schema = contract_block.get("schema", {})

        fields_in = (
            raw_schema
            if isinstance(raw_schema, list)
            else (raw_schema.get("fields", []) if isinstance(raw_schema, dict) else [])
        )

        properties: List[Dict[str, Any]] = []
        for f in fields_in:
            if not isinstance(f, dict):
                continue
            prop: Dict[str, Any] = {
                "name": f.get("name", f.get("id", "unnamed")),
                "logicalType": odcs_logical_type(f.get("type", "string")),
            }
            if f.get("description"):
                prop["description"] = f["description"]
            if f.get("required") is not None:
                prop["required"] = bool(f["required"])
            if f.get("primaryKey") or f.get("primary_key"):
                prop["primaryKey"] = True
            if f.get("sensitivity"):
                prop["classification"] = f["sensitivity"]
            properties.append(prop)

        if properties:
            schema_entry: Dict[str, Any] = {
                "name": model_id,
                "physicalType": expose.get("kind") or expose.get("type", "table"),
                "properties": properties,
            }
            schema_array.append(schema_entry)

        provider = extract_provider(expose)
        binding = expose.get("binding", {})
        if isinstance(binding, dict) and binding:
            srv: Dict[str, Any] = {}
            if provider:
                srv["type"] = PROVIDER_TYPE_MAP.get(provider.lower(), provider.title()).lower()
            location = binding.get("location", {})
            if isinstance(location, dict):
                for k, v in location.items():
                    if v and not str(v).startswith("{{"):
                        srv[k] = v
            fmt_val = binding.get("format")
            if fmt_val:
                srv["format"] = str(fmt_val)
            if srv:
                servers.append(srv)

    if schema_array:
        dc["schema"] = schema_array
    if servers:
        dc["servers"] = servers

    sla = fluid.get("sla", meta.get("sla", {}))
    if isinstance(sla, dict) and sla:
        slo: Dict[str, Any] = {}
        if "freshness" in sla:
            slo["freshness"] = sla["freshness"]
        if "availability" in sla:
            slo["availability"] = sla["availability"]
        if "completeness" in sla:
            slo["completeness"] = sla["completeness"]
        if slo:
            dc["serviceLevelObjectives"] = slo

    tags: List[str] = []
    top_tags = fluid.get("tags", [])
    if isinstance(top_tags, list):
        tags.extend(top_tags)
    meta_tags = meta.get("tags", [])
    if isinstance(meta_tags, list):
        for tag in meta_tags:
            if tag not in tags:
                tags.append(tag)
    if tags:
        dc["tags"] = tags

    custom_props: List[Dict[str, Any]] = []
    labels = fluid.get("labels", {})
    if isinstance(labels, dict):
        for k, v in labels.items():
            custom_props.append({"property": k, "value": v})
    builds = fluid.get("builds", {})
    if isinstance(builds, (dict, list)) and builds:
        custom_props.append({"property": "builds", "value": builds})
    if custom_props:
        dc["customProperties"] = custom_props

    return dc


def build_data_contract_dcs(
    fluid: Mapping[str, Any], product_id: str
) -> Dict[str, Any]:
    """Build a Data Contract Specification 0.9.3 payload (deprecated)."""
    meta = fluid.get("metadata", {})
    contract_id = f"{product_id}-contract"

    dc: Dict[str, Any] = {
        "dataContractSpecification": "0.9.3",
        "id": contract_id,
        "info": {
            "title": meta.get("name") or fluid.get("name") or product_id,
            "version": meta.get("version", "1.0.0"),
            "description": meta.get("description") or fluid.get("description", ""),
            "owner": derive_team_id(fluid),
        },
    }

    domain = fluid.get("domain") or meta.get("domain")
    if domain:
        dc["info"]["domain"] = str(domain)

    models: Dict[str, Any] = {}
    servers: Dict[str, Any] = {}
    all_dq_rules: List[Dict[str, Any]] = []

    for expose in fluid.get("exposes", []):
        model_id = expose.get("id", expose.get("exposeId", "default"))

        schema = expose.get("schema", {})
        if not schema:
            contract_block = expose.get("contract", {})
            if isinstance(contract_block, dict):
                schema = contract_block.get("schema", {})

        fields_in = (
            schema
            if isinstance(schema, list)
            else (schema.get("fields", []) if isinstance(schema, dict) else [])
        )
        fields_out: Dict[str, Any] = {}
        for f in fields_in:
            if not isinstance(f, dict):
                continue
            fname = f.get("name", f.get("id", "unnamed"))
            fdef: Dict[str, Any] = {"type": f.get("type", "string")}
            if f.get("description"):
                fdef["description"] = f["description"]
            if f.get("required") is not None:
                fdef["required"] = bool(f["required"])
            if f.get("sensitivity"):
                fdef["classification"] = f["sensitivity"]
            fields_out[fname] = fdef
        if fields_out:
            models[model_id] = {
                "type": expose.get("kind") or expose.get("type", "table"),
                "fields": fields_out,
            }

        binding = expose.get("binding", {})
        if isinstance(binding, dict) and binding:
            provider = extract_provider(expose)
            server_entry: Dict[str, Any] = {}

            if provider:
                server_entry["type"] = PROVIDER_TYPE_MAP.get(
                    provider.lower(), provider.title()
                )

            location = binding.get("location", {})
            if isinstance(location, dict):
                for k, v in location.items():
                    if v and not str(v).startswith("{{"):
                        server_entry[k] = v

            fmt_val = binding.get("format")
            if fmt_val:
                server_entry["format"] = str(fmt_val)

            if server_entry:
                servers[model_id] = server_entry

        policy = expose.get("policy", {})
        dq = policy.get("dq", {}) if isinstance(policy, dict) else {}
        rules = dq.get("rules", []) if isinstance(dq, dict) else []
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            dq_entry: Dict[str, Any] = {
                "type": rule.get("type", "custom"),
                "description": rule.get("description", ""),
            }
            if rule.get("id"):
                dq_entry["id"] = rule["id"]
            if rule.get("severity"):
                dq_entry["severity"] = rule["severity"]
            if rule.get("selector"):
                dq_entry["field"] = rule["selector"]
            if rule.get("threshold") is not None:
                dq_entry["threshold"] = rule["threshold"]
            if rule.get("window"):
                dq_entry["window"] = rule["window"]
            all_dq_rules.append(dq_entry)

    if models:
        dc["models"] = models
    if servers:
        dc["servers"] = servers

    quality: Dict[str, Any] = {}
    sla = fluid.get("sla", meta.get("sla", {}))
    if isinstance(sla, dict) and sla:
        if "freshness" in sla:
            quality["freshness"] = sla["freshness"]
        if "availability" in sla:
            quality["availability"] = sla["availability"]
        if "completeness" in sla:
            quality["completeness"] = sla["completeness"]
    if all_dq_rules:
        quality["checks"] = all_dq_rules
    if quality:
        dc["quality"] = quality

    builds = fluid.get("builds", {})
    if isinstance(builds, dict) and builds:
        dc["custom"] = dc.get("custom", {})
        dc["custom"]["builds"] = builds

    tags = fluid.get("tags", [])
    if not tags:
        tags = meta.get("tags", [])
    labels = fluid.get("labels", {})
    if tags or labels:
        dc["custom"] = dc.get("custom", {})
        if tags:
            dc["custom"]["tags"] = tags
        if isinstance(labels, dict) and labels:
            dc["custom"]["labels"] = labels

    return dc


def summarize_odcs_payload(odcs_body: Mapping[str, Any]) -> Dict[str, int]:
    schema = odcs_body.get("schema", [])
    servers = odcs_body.get("servers", [])
    sla_properties = odcs_body.get("slaProperties", [])

    schema_objects = len(schema) if isinstance(schema, list) else 0
    schema_properties = 0
    if isinstance(schema, list):
        for schema_object in schema:
            if not isinstance(schema_object, Mapping):
                continue
            properties = schema_object.get("properties", [])
            if isinstance(properties, list):
                schema_properties += len(properties)

    return {
        "schema_objects": schema_objects,
        "schema_properties": schema_properties,
        "servers": len(servers) if isinstance(servers, list) else 0,
        "sla_properties": len(sla_properties) if isinstance(sla_properties, list) else 0,
    }
