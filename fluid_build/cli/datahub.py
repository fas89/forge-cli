"""DataHub CLI command."""

from __future__ import annotations

import json
import logging

from fluid_build.cli.bootstrap import load_contract_with_overlay
from fluid_build.cli.console import cprint, success
from fluid_build.cli.console import error as console_error
from fluid_build.providers.base import ProviderError
from fluid_build.providers.datahub import DataHubProvider


def add_parser(subparsers):
    """Add datahub subcommand."""

    parser = subparsers.add_parser("datahub", help="Publish to DataHub")
    datahub_sub = parser.add_subparsers(dest="datahub_command")

    pub = datahub_sub.add_parser("publish", help="Publish data product to DataHub")
    pub.add_argument("contract", help="Path to FLUID contract file")
    pub.add_argument("-o", "--overlay", help="Path to overlay file")
    pub.add_argument("--server-url", help="DataHub server URL")
    pub.add_argument("--token", help="DataHub API token")
    pub.add_argument("--environment", default="PROD", help="DataHub environment label")
    pub.add_argument("--dry-run", action="store_true", help="Preview without publishing")
    pub.set_defaults(func=_cmd_publish)

    ls = datahub_sub.add_parser("list", help="List DataHub data products")
    ls.add_argument("--server-url", help="DataHub server URL")
    ls.add_argument("--token", help="DataHub API token")
    ls.add_argument("--format", "-f", choices=["text", "json"], default="text")
    ls.set_defaults(func=_cmd_list)

    gt = datahub_sub.add_parser("get", help="Get DataHub data product by URN or id")
    gt.add_argument("product_id", help="Data product URN or contract id")
    gt.add_argument("--server-url", help="DataHub server URL")
    gt.add_argument("--token", help="DataHub API token")
    gt.add_argument("--format", "-f", choices=["text", "json"], default="text")
    gt.set_defaults(func=_cmd_get)

    return parser


def _make_provider(args) -> DataHubProvider:
    kwargs = {}
    if getattr(args, "server_url", None):
        kwargs["server_url"] = args.server_url
    if getattr(args, "token", None):
        kwargs["token"] = args.token
    if getattr(args, "environment", None):
        kwargs["environment"] = args.environment
    return DataHubProvider(**kwargs)


def _cmd_publish(args, logger=None):
    try:
        contract = load_contract_with_overlay(
            args.contract,
            getattr(args, "overlay", None),
            logger or logging.getLogger(__name__),
        )
        result = _make_provider(args).apply(contract, dry_run=getattr(args, "dry_run", False))
        if getattr(args, "dry_run", False):
            cprint(json.dumps(result, indent=2, sort_keys=True))
            return 0
        success(f"Published DataHub product {result.get('product_urn', contract.get('id', '?'))}")
        if result.get("warnings"):
            cprint(json.dumps({"warnings": result["warnings"]}, indent=2))
        return 0
    except ProviderError as exc:
        console_error(f"Error: {exc}")
        return 1
    except Exception as exc:
        console_error(f"Error: {exc}")
        return 1


def _cmd_list(args, logger=None):
    try:
        result = _make_provider(args).list_products()
        if getattr(args, "format", "text") == "json":
            cprint(json.dumps(result, indent=2, sort_keys=True))
        else:
            for item in result:
                cprint(f"{item.get('urn', '?')} {item.get('name', '')}".strip())
        return 0
    except Exception as exc:
        console_error(f"Error: {exc}")
        return 1


def _cmd_get(args, logger=None):
    try:
        result = _make_provider(args).get_product(args.product_id)
        if getattr(args, "format", "text") == "json":
            cprint(json.dumps(result, indent=2, sort_keys=True))
        else:
            cprint(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        console_error(f"Error: {exc}")
        return 1
