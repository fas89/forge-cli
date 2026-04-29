from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fluid_build.cli import atlan as atlan_cli
from fluid_build.cli import collibra as collibra_cli
from fluid_build.cli import datahub as datahub_cli


def test_marketplace_parsers_register_publish_subcommands():
    root = argparse.ArgumentParser()
    subparsers = root.add_subparsers(dest="cmd")
    datahub_cli.add_parser(subparsers)
    atlan_cli.add_parser(subparsers)
    collibra_cli.add_parser(subparsers)

    assert root.parse_args(["datahub", "publish", "contract.yaml"]).datahub_command == "publish"
    assert root.parse_args(["atlan", "publish", "contract.yaml"]).atlan_command == "publish"
    assert root.parse_args(["collibra", "publish", "contract.yaml"]).collibra_command == "publish"


@patch.object(datahub_cli, "load_contract_with_overlay", return_value={"id": "example"})
@patch.object(datahub_cli, "DataHubProvider")
def test_datahub_cli_publish_calls_apply(mock_provider_cls, mock_loader):
    provider = MagicMock()
    provider.apply.return_value = {"product_urn": "urn:li:dataProduct:example"}
    mock_provider_cls.return_value = provider

    code = datahub_cli._cmd_publish(
        SimpleNamespace(
            contract="contract.yaml",
            overlay=None,
            server_url=None,
            token=None,
            environment="PROD",
            dry_run=False,
        )
    )

    assert code == 0
    mock_loader.assert_called_once()
    provider.apply.assert_called_once()


@patch.object(atlan_cli, "load_contract_with_overlay", return_value={"id": "example"})
@patch.object(atlan_cli, "AtlanProvider")
def test_atlan_cli_publish_calls_apply(mock_provider_cls, mock_loader):
    provider = MagicMock()
    provider.apply.return_value = {"qualified_name": "default/product/example"}
    mock_provider_cls.return_value = provider

    code = atlan_cli._cmd_publish(
        SimpleNamespace(
            contract="contract.yaml",
            overlay=None,
            base_url=None,
            api_key=None,
            dry_run=False,
        )
    )

    assert code == 0
    mock_loader.assert_called_once()
    provider.apply.assert_called_once()


@patch.object(collibra_cli, "load_contract_with_overlay", return_value={"id": "example"})
@patch.object(collibra_cli, "CollibraProvider")
def test_collibra_cli_publish_calls_apply(mock_provider_cls, mock_loader):
    provider = MagicMock()
    provider.apply.return_value = {"success": True}
    mock_provider_cls.return_value = provider

    code = collibra_cli._cmd_publish(
        SimpleNamespace(
            contract="contract.yaml",
            overlay=None,
            base_url=None,
            username=None,
            password=None,
            dry_run=False,
        )
    )

    assert code == 0
    mock_loader.assert_called_once()
    provider.apply.assert_called_once()
