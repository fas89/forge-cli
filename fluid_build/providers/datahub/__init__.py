"""DataHub provider package."""

from fluid_build.providers.datahub.datahub import DataHubProvider

try:
    from fluid_build.providers import register_provider

    register_provider("datahub", DataHubProvider)
except Exception:
    pass

NAME = "datahub"
Provider = DataHubProvider

__all__ = ["DataHubProvider", "NAME", "Provider"]
