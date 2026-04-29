"""Collibra provider package."""

from fluid_build.providers.collibra.collibra import CollibraProvider

try:
    from fluid_build.providers import register_provider

    register_provider("collibra", CollibraProvider)
except Exception:
    pass

NAME = "collibra"
Provider = CollibraProvider

__all__ = ["CollibraProvider", "NAME", "Provider"]
