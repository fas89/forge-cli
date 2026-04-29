"""Atlan CLI command."""

from __future__ import annotations

import json
import logging

from fluid_build.cli.bootstrap import load_contract_with_overlay
from fluid_build.cli.console import cprint, success
from fluid_build.cli.console import error as console_error
from fluid_build.providers.atlan import AtlanProvider
from fluid_build.providers.base import ProviderError


def add_parser(subparsers):
    """Add atlan subcommand."""

    parser = subparsers.add_parser("atlan", help="Publish to Atlan")
    atlan_sub = parser.add_subparsers(dest="atlan_command")

    pub = atlan_sub.add_parser("publish", help="Publish data product to Atlan")
    pub.add_argument("contract", help="Path to FLUID contract file")
    pub.add_argument("-o", "--overlay", help="Path to overlay file")
    pub.add_argument("--base-url", help="Atlan base URL")
    pub.add_argument("--api-key", help="Atlan API key")
    pub.add_argument("--dry-run", action="store_true", help="Preview without publishing")
    pub.set_defaults(func=_cmd_publish)

    ls = atlan_sub.add_parser("list", help="List Atlan data products")
    ls.add_argument("--base-url", help="Atlan base URL")
    ls.add_argument("--api-key", help="Atlan API key")
    ls.add_argument("--format", "-f", choices=["text", "json"], default="text")
    ls.set_defaults(func=_cmd_list)

    gt = atlan_sub.add_parser("get", help="Get Atlan data product by qualified name or id")
    gt.add_argument("product_id", help="Qualified name or FLUID contract id")
    gt.add_argument("--base-url", help="Atlan base URL")
    gt.add_argument("--api-key", help="Atlan API key")
    gt.add_argument("--format", "-f", choices=["text", "json"], default="text")
    gt.set_defaults(func=_cmd_get)

    return parser


def _make_provider(args) -> AtlanProvider:
    kwargs = {}
    if getattr(args, "base_url", None):
        kwargs["base_url"] = args.base_url
    if getattr(args, "api_key", None):
        kwargs["api_key"] = args.api_key
    return AtlanProvider(**kwargs)


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
        success(
            f"Published Atlan product {result.get('qualified_name', contract.get('id', '?'))}"
        )
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
                attrs = item.get("attributes", {})
                cprint(f"{attrs.get('qualifiedName', '?')} {attrs.get('name', '')}".strip())
        return 0
    except Exception as exc:
        console_error(f"Error: {exc}")
        return 1


def _cmd_get(args, logger=None):
    try:
        result = _make_provider(args).get_product(args.product_id)
        cprint(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        console_error(f"Error: {exc}")
        return 1
