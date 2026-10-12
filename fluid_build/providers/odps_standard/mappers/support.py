# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Support channels and management ports (opaque pass-through).

FLUID has no native model for support channels or management endpoints, so
both flow through ``metadata.odps_passthrough.{support, management_ports}``
verbatim.
"""

from __future__ import annotations

import copy

from .base import (
    ExportCtx,
    ImportCtx,
    get_metadata_passthrough,
    metadata_passthrough,
)

# Deep copies throughout: a stash sharing objects with the source document
# (also stashed) would make the YAML dump of the imported FLUID emit
# anchors/aliases, and a rendered document must never alias the FLUID.


def to_odps(ctx: ExportCtx) -> None:
    pt = get_metadata_passthrough(ctx.fluid)
    if "support" in pt:
        ctx.odps["support"] = copy.deepcopy(list(pt["support"]))
    if "management_ports" in pt:
        ctx.odps["managementPorts"] = copy.deepcopy(list(pt["management_ports"]))


def to_fluid(ctx: ImportCtx) -> None:
    pt = metadata_passthrough(ctx.fluid)
    if ctx.odps.get("support"):
        pt["support"] = copy.deepcopy(list(ctx.odps["support"]))
    if ctx.odps.get("managementPorts"):
        pt["management_ports"] = copy.deepcopy(list(ctx.odps["managementPorts"]))
