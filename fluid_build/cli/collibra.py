"""Collibra CLI command."""

from __future__ import annotations

import json
import logging

from fluid_build.cli.bootstrap import load_contract_with_overlay
from fluid_build.cli.console import cprint, success
from fluid_build.cli.console import error as console_error
from fluid_build.providers.base import ProviderError
from fluid_build.providers.collibra import CollibraProvider


def add_parser(subparsers):
    """Add collibra subcommand."""

    parser = subparsers.add_parser("collibra", help="Publish to Collibra")
    coll_sub = parser.add_subparsers(dest="collibra_command")

    pub = coll_sub.add_parser("publish", help="Publish data product to Collibra")
    pub.add_argument("contract", help="Path to FLUID contract file")
    pub.add_argument("-o", "--overlay", help="Path to overlay file")
    pub.add_argument("--base-url", help="Collibra base URL")
    pub.add_argument("--token", help="Collibra bearer token")
    pub.add_argument("--username", help="Collibra username")
    pub.add_argument("--password", help="Collibra password")
    pub.add_argument("--dry-run", action="store_true", help="Preview without publishing")
    pub.set_defaults(func=_cmd_publish)

    ls = coll_sub.add_parser("list", help="List Collibra data products")
    ls.add_argument("--base-url", help="Collibra base URL")
    ls.add_argument("--token", help="Collibra bearer token")
    ls.add_argument("--username", help="Collibra username")
    ls.add_argument("--password", help="Collibra password")
    ls.add_argument("--format", "-f", choices=["text", "json"], default="text")
    ls.set_defaults(func=_cmd_list)

    gt = coll_sub.add_parser("get", help="Get Collibra data product by id")
    gt.add_argument("product_id", help="FLUID contract id")
    gt.add_argument("--base-url", help="Collibra base URL")
    gt.add_argument("--token", help="Collibra bearer token")
    gt.add_argument("--username", help="Collibra username")
    gt.add_argument("--password", help="Collibra password")
    gt.add_argument("--format", "-f", choices=["text", "json"], default="text")
    gt.set_defaults(func=_cmd_get)

    return parser


def _make_provider(args) -> CollibraProvider:
    kwargs = {}
    if getattr(args, "base_url", None):
        kwargs["base_url"] = args.base_url
    if getattr(args, "token", None):
        kwargs["token"] = args.token
    if getattr(args, "username", None):
        kwargs["username"] = args.username
    if getattr(args, "password", None):
        kwargs["password"] = args.password
    return CollibraProvider(**kwargs)


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
        success(f"Published Collibra product {contract.get('id', '?')}")
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
                cprint(f"{item.get('name', '?')} ({item.get('id', item.get('externalEntityId', ''))})")
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
