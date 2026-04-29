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

"""Base catalog provider for marketplace integrations.

Similar to market.py's BaseCatalogConnector but focused on write-side
operations (publish, update, delete).
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class CatalogOwner:
    """Normalized owner representation for catalog publishing."""

    team: str
    email: str = ""
    name: str = ""
    id: str = ""

    @property
    def display_name(self) -> str:
        return self.name or self.team or self.email or "unknown"


@dataclass
class CatalogContractRef:
    """Reference to a contract associated with a product or port."""

    id: str
    name: str = ""
    port_id: str = ""
    format: str = ""
    url: str = ""
    kind: str = "contract"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CatalogAssetRef:
    """Reference to a related asset/member for future marketplace adapters."""

    id: str
    name: str = ""
    kind: str = ""
    relation: str = ""
    platform: str = ""
    location: Any = None
    external_ref: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CatalogPort:
    """Normalized input/output port representation."""

    id: str
    name: str
    direction: str
    description: str = ""
    kind: str = ""
    platform: str = "unknown"
    location: Any = None
    schema: Optional[List[Dict[str, Any]]] = None
    tags: List[str] = field(default_factory=list)
    links: Dict[str, str] = field(default_factory=dict)
    custom: Dict[str, Any] = field(default_factory=dict)
    status: str = "active"
    sensitivity: str = "internal"
    contains_pii: bool = False
    source_system_id: str = ""
    contract_ref: Optional[CatalogContractRef] = None


@dataclass
class CatalogProduct:
    """Normalized product graph representation for catalog publishing."""

    id: str  # contract.id
    name: str  # contract.name
    description: str  # contract.description
    kind: str  # contract.kind (e.g., 'DataProduct', 'DataStream')
    domain: str  # contract.domain
    owner: CatalogOwner
    version: str  # contract.version
    layer: str  # contract.metadata.layer (Bronze/Silver/Gold)
    status: str = "draft"
    tags: List[str] = field(default_factory=list)
    sensitivity: str = "internal"  # internal, public, confidential
    links: Dict[str, str] = field(default_factory=dict)
    custom: Dict[str, Any] = field(default_factory=dict)
    input_ports: List[CatalogPort] = field(default_factory=list)
    output_ports: List[CatalogPort] = field(default_factory=list)
    contracts: List[CatalogContractRef] = field(default_factory=list)
    members: List[CatalogAssetRef] = field(default_factory=list)
    source_contract: Optional[Dict[str, Any]] = None
    contract_yaml: Optional[str] = None  # Raw YAML content of the contract file

    @property
    def type(self) -> str:
        """Compatibility alias for older asset-centric integrations."""
        return str(self.kind or "DataProduct").lower()

    @property
    def owner_email(self) -> str:
        return self.owner.email

    @property
    def primary_output_port(self) -> Optional[CatalogPort]:
        return self.output_ports[0] if self.output_ports else None

    @property
    def platform(self) -> str:
        port = self.primary_output_port or (self.input_ports[0] if self.input_ports else None)
        return port.platform if port else "unknown"

    @property
    def location(self) -> Any:
        port = self.primary_output_port or (self.input_ports[0] if self.input_ports else None)
        return port.location if port else {}

    @property
    def schema(self) -> Optional[List[Dict[str, Any]]]:
        port = self.primary_output_port
        return port.schema if port else None


# Backward compatibility alias while the catalog integration surface moves from
# flat assets to richer product graphs.
CatalogAsset = CatalogProduct


@dataclass
class PublishResult:
    """Result of catalog publish operation"""

    success: bool
    catalog_id: str
    asset_id: str
    catalog_url: Optional[str] = None
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


class BaseCatalogProvider(ABC):
    """Base class for all catalog integrations

    Provides common functionality:
    - Contract to asset mapping
    - Authentication setup
    - Error handling patterns
    - Logging

    Subclasses implement catalog-specific operations.
    """

    name: str = "base"  # Override in subclasses

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.endpoint = config.get("endpoint", config.get("url", ""))
        self.auth = config.get("auth", {})
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Resilience settings
        self.max_retries = config.get("max_retries", 3)
        self.timeout = config.get("timeout", 30.0)

    @abstractmethod
    async def publish(self, product: CatalogProduct) -> PublishResult:
        """Publish product to catalog

        Args:
            product: Normalized product to publish

        Returns:
            PublishResult with success/failure details
        """
        pass

    @abstractmethod
    async def update(self, product: CatalogProduct) -> PublishResult:
        """Update existing product in catalog

        Args:
            product: Normalized product with updated information

        Returns:
            PublishResult with success/failure details
        """
        pass

    @abstractmethod
    async def verify(self, asset_id: str) -> bool:
        """Verify asset exists in catalog

        Args:
            asset_id: Unique identifier of the asset

        Returns:
            True if asset exists, False otherwise
        """
        pass

    async def delete(self, asset_id: str) -> bool:
        """Delete asset from catalog (optional)

        Args:
            asset_id: Unique identifier of the asset

        Returns:
            True if deleted, False otherwise
        """
        self.logger.warning(f"Delete not implemented for {self.name}")
        return False

    def map_contract_to_product(self, contract: Dict[str, Any]) -> CatalogProduct:
        """Map FLUID contract to a normalized catalog product graph.

        This is the common mapping shared by publish-side marketplace
        integrations. Subclasses can override it for custom mapping logic.

        Args:
            contract: FLUID contract dictionary

        Returns:
            CatalogProduct instance
        """
        metadata = self._as_dict(contract.get("metadata"))
        owner = self._normalize_owner(contract.get("owner", metadata.get("owner")))
        product_id = str(contract.get("id", contract.get("name", "unknown")))
        name = str(contract.get("name") or metadata.get("name") or product_id)
        description = str(contract.get("description") or metadata.get("description") or "")
        kind = str(contract.get("kind", "DataProduct"))
        domain = str(contract.get("domain") or metadata.get("domain") or "general")
        version = str(contract.get("version") or metadata.get("version") or "1.0.0")
        layer = str(metadata.get("layer") or "Bronze")
        status = str(metadata.get("status") or contract.get("status") or "draft")

        input_ports = self._normalize_input_ports(contract)
        output_ports = self._normalize_output_ports(contract, product_id)
        contracts = self._normalize_contract_refs(contract, output_ports)
        members = self._normalize_members(contract)

        return CatalogProduct(
            id=product_id,
            name=name,
            description=description,
            kind=kind,
            domain=domain,
            owner=owner,
            version=version,
            layer=layer,
            status=status,
            tags=self._merge_unique_strings(contract.get("tags"), metadata.get("tags")),
            sensitivity=self._infer_sensitivity(contract, metadata, output_ports),
            links=self._extract_links(contract, metadata),
            custom=self._extract_custom(contract, metadata),
            input_ports=input_ports,
            output_ports=output_ports,
            contracts=contracts,
            members=members,
            source_contract=dict(contract),
        )

    def map_contract_to_asset(self, contract: Dict[str, Any]) -> CatalogAsset:
        """Deprecated asset-centric alias for the richer product mapper."""
        return self.map_contract_to_product(contract)

    def validate_product(self, product: CatalogProduct) -> tuple[bool, Optional[str]]:
        """Validate product before publishing.

        Args:
            product: Product to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not product.name:
            return False, "Product name is required"

        if not product.id:
            return False, "Product ID is required"

        if not product.owner.display_name:
            return False, "Product owner is required"

        if not product.domain:
            return False, "Product domain is required"

        return True, None

    def validate_asset(self, asset: CatalogAsset) -> tuple[bool, Optional[str]]:
        """Deprecated asset-centric alias for product validation."""
        return self.validate_product(asset)

    async def health_check(self) -> bool:
        """Check if catalog is accessible

        Returns:
            True if healthy, False otherwise
        """
        try:
            # Default implementation - subclasses should override
            return True
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False

    @staticmethod
    def _as_dict(value: Any) -> Dict[str, Any]:
        return dict(value) if isinstance(value, Mapping) else {}

    @staticmethod
    def _merge_unique_strings(*values: Any) -> List[str]:
        merged: List[str] = []
        for value in values:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
                continue
            for item in value:
                if isinstance(item, str) and item not in merged:
                    merged.append(item)
        return merged

    @staticmethod
    def _normalize_owner(value: Any) -> CatalogOwner:
        if isinstance(value, Mapping):
            team = str(value.get("team") or value.get("name") or value.get("id") or "unknown")
            email = str(value.get("email") or "")
            name = str(value.get("name") or team or "")
            owner_id = str(value.get("id") or team or name)
            return CatalogOwner(team=team, email=email, name=name, id=owner_id)

        if isinstance(value, str):
            raw = value.strip()
            if raw:
                if "@" in raw:
                    local_part = raw.split("@", 1)[0]
                    normalized = local_part.replace(".", "-").replace("_", "-")
                    return CatalogOwner(team=normalized, email=raw, name=local_part, id=normalized)
                return CatalogOwner(team=raw, name=raw, id=raw)

        return CatalogOwner(team="unknown", name="unknown", id="unknown")

    def _normalize_input_ports(self, contract: Mapping[str, Any]) -> List[CatalogPort]:
        ports: List[CatalogPort] = []
        sections: List[Mapping[str, Any]] = []
        for key in ("expects", "consumes"):
            value = contract.get(key, [])
            if isinstance(value, list):
                sections.extend(item for item in value if isinstance(item, Mapping))

        for index, section in enumerate(sections):
            port_id = self._first_string(
                section.get("id"),
                section.get("name"),
                section.get("productId"),
                section.get("ref"),
            ) or f"input-{index}"
            name = self._first_string(section.get("name"), section.get("title"), port_id) or port_id
            schema = self._normalize_schema(section)
            provider = self._extract_provider(section)
            port = CatalogPort(
                id=port_id,
                name=name,
                direction="input",
                description=str(section.get("description") or ""),
                kind=str(section.get("kind") or ""),
                platform=provider or "unknown",
                location=self._extract_location(section, provider),
                schema=schema,
                tags=self._merge_unique_strings(section.get("tags")),
                links=self._extract_section_links(section),
                custom=self._extract_section_custom(section),
                status=self._extract_section_status(section, default="active"),
                sensitivity=self._extract_section_sensitivity(section),
                contains_pii=self._contains_pii(schema),
                source_system_id=self._first_string(
                    section.get("source_system"),
                    section.get("sourceSystem"),
                    section.get("productId"),
                    section.get("ref"),
                )
                or "",
            )
            ports.append(port)

        return ports

    def _normalize_output_ports(
        self, contract: Mapping[str, Any], product_id: str
    ) -> List[CatalogPort]:
        ports: List[CatalogPort] = []
        exposes = contract.get("exposes", [])
        if not isinstance(exposes, list):
            return ports

        for index, expose in enumerate(exposes):
            if not isinstance(expose, Mapping):
                continue
            port_id = self._first_string(expose.get("id"), expose.get("exposeId")) or f"output-{index}"
            name = (
                self._first_string(expose.get("name"), expose.get("title"), expose.get("description"))
                or port_id
            )
            schema = self._normalize_schema(expose)
            provider = self._extract_provider(expose)
            contract_ref = CatalogContractRef(
                id=f"{product_id}.{port_id}",
                name=f"{name} contract",
                port_id=port_id,
                format="fluid",
                metadata={"source": "expose"},
            )
            ports.append(
                CatalogPort(
                    id=port_id,
                    name=name,
                    direction="output",
                    description=str(expose.get("description") or ""),
                    kind=str(expose.get("kind") or ""),
                    platform=provider or "unknown",
                    location=self._extract_location(expose, provider),
                    schema=schema,
                    tags=self._merge_unique_strings(expose.get("tags")),
                    links=self._extract_section_links(expose),
                    custom=self._extract_section_custom(expose),
                    status=self._extract_section_status(expose, default="active"),
                    sensitivity=self._extract_section_sensitivity(expose),
                    contains_pii=self._contains_pii(schema),
                    contract_ref=contract_ref,
                )
            )

        return ports

    def _normalize_contract_refs(
        self, contract: Mapping[str, Any], output_ports: Sequence[CatalogPort]
    ) -> List[CatalogContractRef]:
        refs: List[CatalogContractRef] = []
        seen: set[str] = set()

        for port in output_ports:
            if port.contract_ref and port.contract_ref.id not in seen:
                refs.append(port.contract_ref)
                seen.add(port.contract_ref.id)

        for key in ("contracts", "dataContracts"):
            raw_refs = contract.get(key, [])
            if not isinstance(raw_refs, list):
                continue
            for raw_ref in raw_refs:
                ref = self._normalize_contract_ref(raw_ref)
                if ref and ref.id not in seen:
                    refs.append(ref)
                    seen.add(ref.id)

        return refs

    def _normalize_members(self, contract: Mapping[str, Any]) -> List[CatalogAssetRef]:
        refs: List[CatalogAssetRef] = []
        for key in ("members", "assets", "relatedAssets", "related_assets"):
            raw_refs = contract.get(key, [])
            if not isinstance(raw_refs, list):
                continue
            for raw_ref in raw_refs:
                ref = self._normalize_asset_ref(raw_ref, default_relation=key)
                if ref:
                    refs.append(ref)
        return refs

    def _extract_links(self, contract: Mapping[str, Any], metadata: Mapping[str, Any]) -> Dict[str, str]:
        links: Dict[str, str] = {}

        metadata_links = metadata.get("links")
        if isinstance(metadata_links, Mapping):
            links.update({str(k): str(v) for k, v in metadata_links.items() if v})

        for key in ("documentation", "repository", "catalog", "dataProduct"):
            value = metadata.get(key)
            if value:
                links[key] = str(value)

        top_links = contract.get("links")
        if isinstance(top_links, Mapping):
            links.update({str(k): str(v) for k, v in top_links.items() if v})

        return links

    def _extract_custom(self, contract: Mapping[str, Any], metadata: Mapping[str, Any]) -> Dict[str, Any]:
        custom: Dict[str, Any] = {}

        for key in (
            "domain",
            "subdomain",
            "environment",
            "version",
            "layer",
            "sla",
            "classification",
            "confidentiality",
            "businessContext",
        ):
            value = metadata.get(key)
            if value is not None:
                custom[key] = value

        for key in ("quality", "observability", "sovereignty", "access_control"):
            value = contract.get(key)
            if value is not None:
                custom[key] = value

        metadata_custom = metadata.get("custom")
        if isinstance(metadata_custom, Mapping):
            custom.update(dict(metadata_custom))

        explicit_custom = contract.get("custom")
        if isinstance(explicit_custom, Mapping):
            custom.update(dict(explicit_custom))

        return custom

    def _infer_sensitivity(
        self,
        contract: Mapping[str, Any],
        metadata: Mapping[str, Any],
        output_ports: Sequence[CatalogPort],
    ) -> str:
        access_control = self._as_dict(contract.get("access_control"))
        candidates = [
            metadata.get("classification"),
            metadata.get("confidentiality"),
            access_control.get("classification"),
        ]
        candidates.extend(port.sensitivity for port in output_ports if port.sensitivity)

        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip().lower()

        return "internal"

    @staticmethod
    def _first_string(*values: Any) -> Optional[str]:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _normalize_contract_ref(self, value: Any) -> Optional[CatalogContractRef]:
        if isinstance(value, str) and value.strip():
            return CatalogContractRef(id=value.strip(), name=value.strip())

        if isinstance(value, Mapping):
            ref_id = self._first_string(value.get("id"), value.get("ref"), value.get("url"))
            if not ref_id:
                return None
            return CatalogContractRef(
                id=ref_id,
                name=self._first_string(value.get("name"), ref_id) or ref_id,
                port_id=self._first_string(value.get("portId"), value.get("port_id")) or "",
                format=self._first_string(value.get("format"), value.get("kind")) or "",
                url=self._first_string(value.get("url")) or "",
                kind=self._first_string(value.get("kind")) or "contract",
                metadata=dict(value.get("metadata", {})) if isinstance(value.get("metadata"), Mapping) else {},
            )

        return None

    def _normalize_asset_ref(self, value: Any, default_relation: str = "") -> Optional[CatalogAssetRef]:
        if isinstance(value, str) and value.strip():
            return CatalogAssetRef(id=value.strip(), name=value.strip(), relation=default_relation)

        if isinstance(value, Mapping):
            ref_id = self._first_string(value.get("id"), value.get("ref"), value.get("external_ref"))
            if not ref_id:
                return None
            return CatalogAssetRef(
                id=ref_id,
                name=self._first_string(value.get("name"), ref_id) or ref_id,
                kind=self._first_string(value.get("kind"), value.get("type")) or "",
                relation=self._first_string(value.get("relation")) or default_relation,
                platform=self._first_string(value.get("platform")) or "",
                location=value.get("location"),
                external_ref=self._first_string(value.get("external_ref"), value.get("ref")) or "",
                metadata=dict(value.get("metadata", {})) if isinstance(value.get("metadata"), Mapping) else {},
            )

        return None

    @staticmethod
    def _extract_provider(section: Mapping[str, Any]) -> str:
        binding = section.get("binding", {})
        if isinstance(binding, Mapping):
            provider = binding.get("platform") or binding.get("provider")
            if provider:
                return str(provider)
        provider = section.get("provider")
        return str(provider) if provider else ""

    def _extract_location(self, section: Mapping[str, Any], provider: str) -> Any:
        binding = section.get("binding", {})
        if isinstance(binding, Mapping) and binding.get("location") is not None:
            return binding.get("location")

        location = section.get("location")
        if location is not None:
            return location

        provider_key = provider.lower() if provider else ""
        for key in (provider_key, "gcp", "bigquery", "snowflake", "aws", "s3", "redshift", "kafka"):
            if not key:
                continue
            nested = section.get(key)
            if nested is not None:
                return nested

        ref_location: Dict[str, Any] = {}
        for key in ("productId", "exposeId", "ref", "path", "uri"):
            value = section.get(key)
            if value is not None:
                ref_location[key] = value
        return ref_location or {}

    def _normalize_schema(self, section: Mapping[str, Any]) -> Optional[List[Dict[str, Any]]]:
        raw_schema: Any = section.get("schema")
        if raw_schema is None:
            contract_block = section.get("contract", {})
            if isinstance(contract_block, Mapping):
                raw_schema = contract_block.get("schema")

        if isinstance(raw_schema, list):
            return [dict(item) for item in raw_schema if isinstance(item, Mapping)]

        if isinstance(raw_schema, Mapping):
            fields = raw_schema.get("fields")
            if isinstance(fields, list):
                return [dict(item) for item in fields if isinstance(item, Mapping)]

            properties = raw_schema.get("properties")
            if isinstance(properties, Mapping):
                normalized: List[Dict[str, Any]] = []
                required = raw_schema.get("required", [])
                required_fields = set(required) if isinstance(required, list) else set()
                for name, value in properties.items():
                    if isinstance(value, Mapping):
                        field_def = dict(value)
                    else:
                        field_def = {"type": value}
                    field_def.setdefault("name", str(name))
                    if name in required_fields:
                        field_def.setdefault("required", True)
                    normalized.append(field_def)
                return normalized

        return None

    @staticmethod
    def _contains_pii(schema: Optional[Sequence[Mapping[str, Any]]]) -> bool:
        if not schema:
            return False

        for field_def in schema:
            if not isinstance(field_def, Mapping):
                continue
            if field_def.get("pii") is True:
                return True
            for key in ("classification", "sensitivity"):
                value = field_def.get(key)
                if isinstance(value, str) and "pii" in value.lower():
                    return True
            tags = field_def.get("tags")
            if isinstance(tags, list) and any(
                isinstance(tag, str) and "pii" in tag.lower() for tag in tags
            ):
                return True

        return False

    @staticmethod
    def _extract_section_links(section: Mapping[str, Any]) -> Dict[str, str]:
        links = section.get("links", {})
        if isinstance(links, Mapping):
            return {str(k): str(v) for k, v in links.items() if v}
        return {}

    @staticmethod
    def _extract_section_custom(section: Mapping[str, Any]) -> Dict[str, Any]:
        custom: Dict[str, Any] = {}
        binding = section.get("binding", {})
        if isinstance(binding, Mapping):
            if binding.get("format") is not None:
                custom["format"] = binding.get("format")
        explicit_custom = section.get("custom", {})
        if isinstance(explicit_custom, Mapping):
            custom.update(dict(explicit_custom))
        for key in ("productId", "exposeId", "ref"):
            value = section.get(key)
            if value is not None:
                custom[key] = value
        return custom

    @staticmethod
    def _extract_section_status(section: Mapping[str, Any], default: str = "active") -> str:
        lifecycle = section.get("lifecycle", {})
        if isinstance(lifecycle, Mapping):
            state = lifecycle.get("state")
            if isinstance(state, str) and state.strip():
                return state.strip()
        status = section.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip()
        return default

    @staticmethod
    def _extract_section_sensitivity(section: Mapping[str, Any]) -> str:
        for key in ("sensitivity", "classification"):
            value = section.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().lower()
        policy = section.get("policy", {})
        if isinstance(policy, Mapping):
            classification = policy.get("classification")
            if isinstance(classification, str) and classification.strip():
                return classification.strip().lower()
        return "internal"
