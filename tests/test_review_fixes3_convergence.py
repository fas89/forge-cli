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

"""Regression tests for the defects the round-2 convergence check found."""

from __future__ import annotations

import time
import warnings
from typing import Any, Dict, List

import pytest

from fluid_build.providers.catalogs import CatalogTarget
from fluid_build.providers.catalogs.fluid_cc import FluidCommandCenterProvider
from fluid_build.providers.odcs.provider import _merge_product_owner
from fluid_build.providers.odps_standard import resolver

# ---------------------------------------------------------------------------
# validate_target must not let a 0.7.8 mapper override abort a --dry-run batch
# ---------------------------------------------------------------------------


class _NameIndexingSubclass(FluidCommandCenterProvider):
    """0.7.8-style subclass: overrides only map_contract_to_asset and, like
    the 0.7.8 base mapper, indexes ``contract['name']``."""

    def map_contract_to_asset(self, contract: Dict[str, Any]):  # type: ignore[override]
        asset = super().map_contract_to_asset(contract)
        asset.name = contract["name"]
        return asset


class TestValidateTargetGuardsLegacyMapper:
    def _provider(self) -> _NameIndexingSubclass:
        return _NameIndexingSubclass({"endpoint": "http://cc"})

    def test_mapper_error_is_reported_not_raised(self) -> None:
        fluid = {"id": "sales.refunds", "metadata": {"name": "Refunds"}}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            ok, err = self._provider().validate_target(fluid, CatalogTarget.from_fluid(fluid))
        assert ok is False
        assert err.startswith("Failed to map contract to asset:")

    def test_mappable_contract_still_validates(self) -> None:
        fluid = {"id": "sales.orders", "name": "Orders"}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            ok, err = self._provider().validate_target(fluid, CatalogTarget.from_fluid(fluid))
        assert (ok, err) == (True, None)


# ---------------------------------------------------------------------------
# Product owner edits reach only per-port owners inherited from the product
# ---------------------------------------------------------------------------


class TestMergeProductOwner:
    PRODUCT_THEN = {"team": "t", "email": "a@x.com"}
    PRODUCT_NOW = {"team": "t", "email": "z@z.com"}

    def test_port_with_its_own_team_keeps_its_identity(self) -> None:
        doc: Dict[str, Any] = {"owner": {"team": "commerce-team", "email": "alice@acme.com"}}
        _merge_product_owner(doc, self.PRODUCT_THEN, self.PRODUCT_NOW)
        assert doc["owner"] == {"team": "commerce-team", "email": "alice@acme.com"}

    def test_port_that_inherited_the_product_owner_follows_the_edit(self) -> None:
        doc: Dict[str, Any] = {"owner": dict(self.PRODUCT_THEN)}
        _merge_product_owner(doc, self.PRODUCT_THEN, self.PRODUCT_NOW)
        assert doc["owner"] == self.PRODUCT_NOW

    def test_only_inherited_keys_change(self) -> None:
        # Same team as the product, but the port has its own contact email.
        doc: Dict[str, Any] = {"owner": {"team": "t", "email": "port@acme.com"}}
        _merge_product_owner(doc, self.PRODUCT_THEN, {"team": "t2", "email": "z@z.com"})
        assert doc["owner"] == {"team": "t2", "email": "port@acme.com"}

    def test_removing_the_product_owner_keeps_a_distinct_port_team(self) -> None:
        doc: Dict[str, Any] = {"owner": {"team": "commerce-team"}}
        _merge_product_owner(doc, self.PRODUCT_THEN, None)
        assert doc["owner"] == {"team": "commerce-team"}

    def test_removing_the_product_owner_removes_an_inherited_one(self) -> None:
        doc: Dict[str, Any] = {"owner": dict(self.PRODUCT_THEN)}
        _merge_product_owner(doc, self.PRODUCT_THEN, None)
        assert "owner" not in doc

    def test_does_not_mutate_the_document_owner_in_place(self) -> None:
        original = {"team": "t", "email": "a@x.com"}
        doc: Dict[str, Any] = {"owner": original}
        _merge_product_owner(doc, self.PRODUCT_THEN, self.PRODUCT_NOW)
        assert original == {"team": "t", "email": "a@x.com"}


# ---------------------------------------------------------------------------
# resolver.urlopen accepts timeout=None (deadline-only)
# ---------------------------------------------------------------------------


class _FakeOpener:
    def __init__(self, seen: List[Any]) -> None:
        self.seen = seen

    def open(self, url: str, timeout: Any = None) -> str:
        self.seen.append(timeout)
        return "response"


class TestUrlopenTimeoutNone:
    def test_none_timeout_uses_the_remaining_deadline(self, monkeypatch) -> None:
        seen: List[Any] = []
        monkeypatch.setattr(
            resolver.urllib.request, "build_opener", lambda *handlers: _FakeOpener(seen)
        )
        out = resolver.urlopen(
            "http://example.test/c.yaml", timeout=None, deadline=time.monotonic() + 5
        )
        assert out == "response"
        assert len(seen) == 1 and 0 < seen[0] <= 5

    @pytest.mark.parametrize("timeout, expected_max", [(2.0, 2.0), (60.0, 5.0)])
    def test_numeric_timeout_is_capped_by_the_deadline(
        self, monkeypatch, timeout: float, expected_max: float
    ) -> None:
        seen: List[Any] = []
        monkeypatch.setattr(
            resolver.urllib.request, "build_opener", lambda *handlers: _FakeOpener(seen)
        )
        resolver.urlopen(
            "http://example.test/c.yaml", timeout=timeout, deadline=time.monotonic() + 5
        )
        assert 0 < seen[0] <= expected_max
