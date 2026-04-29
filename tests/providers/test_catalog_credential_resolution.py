from __future__ import annotations

from unittest.mock import patch

from fluid_build.providers.atlan import AtlanProvider
from fluid_build.providers.collibra import CollibraProvider
from fluid_build.providers.datahub import DataHubProvider
from fluid_build.providers.datamesh_manager.datamesh_manager import DataMeshManagerProvider


@patch("fluid_build.providers.datahub.datahub.CatalogCredentialAdapter.get_credential", return_value="resolver-token")
def test_datahub_provider_resolves_token_from_credential_adapter(mock_get_credential):
    provider = DataHubProvider(server_url="https://datahub.example.com")

    assert provider.token == "resolver-token"
    mock_get_credential.assert_called_once()


@patch("fluid_build.providers.atlan.atlan.CatalogCredentialAdapter.get_credential", return_value="resolver-key")
def test_atlan_provider_resolves_api_key_from_credential_adapter(mock_get_credential):
    provider = AtlanProvider(base_url="https://tenant.atlan.com")

    assert provider.api_key == "resolver-key"
    mock_get_credential.assert_called_once()


@patch(
    "fluid_build.providers.collibra.collibra.CatalogCredentialAdapter.get_credential",
    side_effect=["resolver-token", "ignored-user", "ignored-password"],
)
def test_collibra_provider_prefers_token_from_credential_adapter(mock_get_credential):
    provider = CollibraProvider(base_url="https://collibra.example.com")

    assert provider.token == "resolver-token"
    assert provider.auth_mode == "bearer"
    assert mock_get_credential.call_count == 3


@patch(
    "fluid_build.providers.datamesh_manager.datamesh_manager.CatalogCredentialAdapter.get_credential",
    return_value="resolver-api-key",
)
def test_dmm_provider_resolves_api_key_from_credential_adapter(mock_get_credential):
    provider = DataMeshManagerProvider(api_url="https://api.entropy-data.com")

    assert provider.api_key == "resolver-api-key"
    mock_get_credential.assert_called_once()
