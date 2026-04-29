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

"""Catalog credential management and status tests for fluid_build.cli.auth."""

from __future__ import annotations

import asyncio
import logging
import os
from unittest.mock import patch

from fluid_build.cli.auth import (
    AuthManager,
    AuthStatus,
    CatalogAuthProvider,
    handle_clear,
    handle_set,
)


def test_catalog_provider_login_guidance_points_to_auth_set():
    provider = CatalogAuthProvider("collibra", {}, logging.getLogger("test"))

    result = asyncio.run(provider.login())

    assert result.status == AuthStatus.ERROR
    assert "fluid auth set --provider collibra --key token" in (result.error_message or "")


def test_catalog_auth_provider_set_secret_uses_keyring_adapter():
    provider = CatalogAuthProvider("datahub", {}, logging.getLogger("test"))

    with patch.object(provider.credential_adapter, "get_stored_credential", return_value=None), patch.object(
        provider.credential_adapter, "store_credential"
    ) as mock_store, patch.object(provider.credential_adapter, "clear_cache") as mock_clear:
        provider.set_secret("token", value="secret-token")

    mock_store.assert_called_once_with("token", "secret-token")
    mock_clear.assert_called_once()


def test_catalog_auth_provider_clear_secret_uses_keyring_adapter():
    provider = CatalogAuthProvider("datahub", {}, logging.getLogger("test"))

    with patch.object(provider.credential_adapter, "get_stored_credential", return_value="secret"), patch.object(
        provider.credential_adapter, "clear_stored_credential"
    ) as mock_clear_stored, patch.object(provider.credential_adapter, "clear_cache") as mock_clear:
        removed = provider.clear_secret("token")

    assert removed is True
    mock_clear_stored.assert_called_once_with("token")
    mock_clear.assert_called_once()


def test_auth_manager_lists_catalog_providers():
    manager = AuthManager(config={}, logger=logging.getLogger("test"))

    providers = manager.list_providers()

    assert "dmm" in providers
    assert "datahub" in providers
    assert "atlan" in providers
    assert "collibra" in providers


def test_datahub_status_reports_env_source_and_identity():
    provider = CatalogAuthProvider("datahub", {}, logging.getLogger("test"))

    with patch.dict(
        os.environ,
        {
            "DATAHUB_TOKEN": "secret",
            "DATAHUB_ENDPOINT": "https://datahub.example.com",
        },
        clear=False,
    ), patch(
        "fluid_build.providers.datahub.datahub.DataHubProvider._graphql",
        return_value={"me": {"urn": "urn:li:corpuser:catalog-user"}},
    ):
        result = asyncio.run(provider.check_auth())

    assert result.status == AuthStatus.AUTHENTICATED
    assert result.user_info["token"] == "present via environment"
    assert result.user_info["endpoint"] == "https://datahub.example.com"
    assert result.user_info["user_urn"] == "urn:li:corpuser:catalog-user"


def test_collibra_status_prefers_bearer_over_basic_when_both_are_present():
    provider = CatalogAuthProvider("collibra", {}, logging.getLogger("test"))

    with patch.dict(
        os.environ,
        {
            "COLLIBRA_TOKEN": "token-secret",
            "COLLIBRA_USERNAME": "user-one",
            "COLLIBRA_PASSWORD": "pass-one",
            "COLLIBRA_ENDPOINT": "https://collibra.example.com",
        },
        clear=False,
    ), patch(
        "fluid_build.providers.collibra.collibra.CollibraProvider._request_json",
        return_value={"results": []},
    ):
        result = asyncio.run(provider.check_auth())

    assert result.status == AuthStatus.AUTHENTICATED
    assert result.user_info["auth_mode"] == "bearer"
    assert result.user_info["token"] == "present via environment"
    assert result.user_info["username"] == "present via environment"
    assert result.user_info["password"] == "present via environment"


def test_handle_set_routes_catalog_credentials_to_manager():
    manager = AuthManager(config={}, logger=logging.getLogger("test"))

    with patch.object(manager, "set_credential") as mock_set:
        code = handle_set(
            "datahub",
            manager,
            logging.getLogger("test"),
            key="token",
            value="secret-token",
            force=False,
        )

    assert code == 0
    mock_set.assert_called_once_with("datahub", "token", value="secret-token", force=False)


def test_handle_clear_routes_catalog_credentials_to_manager():
    manager = AuthManager(config={}, logger=logging.getLogger("test"))

    with patch.object(manager, "clear_credential", return_value=True) as mock_clear:
        code = handle_clear(
            "datahub",
            manager,
            logging.getLogger("test"),
            key="token",
        )

    assert code == 0
    mock_clear.assert_called_once_with("datahub", "token")
