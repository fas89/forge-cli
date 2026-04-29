"""Atlan provider package."""

from fluid_build.providers.atlan.atlan import AtlanProvider

try:
    from fluid_build.providers import register_provider

    register_provider("atlan", AtlanProvider)
except Exception:
    pass

NAME = "atlan"
Provider = AtlanProvider

__all__ = ["AtlanProvider", "NAME", "Provider"]
