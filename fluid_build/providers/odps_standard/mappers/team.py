# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Team object — Bitol ODPS schema is byte-identical to Bitol ODCS Team
(minus the optional ``id`` StableId). We delegate to the ODCS team mapper
to avoid duplicating the FLUID owner ↔ team object translation.

Round-trip follows the ODCS team mapper: the imported team is stashed
verbatim under ``metadata.odps_passthrough.team`` and replayed unless the
FLUID ``owner`` changed since import; an edit is merged onto the stash,
keeping every key FLUID does not model.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping

from fluid_build.providers.odcs.mappers import team as _odcs_team

from .base import (
    ExportCtx,
    ImportCtx,
    get_metadata_passthrough,
    metadata_passthrough,
)


def to_odps(ctx: ExportCtx) -> None:
    pt_team = get_metadata_passthrough(ctx.fluid).get("team")
    if isinstance(pt_team, Mapping) and pt_team:
        # Imported product: three-way merge (stash + FLUID owner edit).
        then = _odcs_team._import_owner(pt_team)
        now = _odcs_team._current_owner(ctx.fluid)
        if now == then:
            ctx.odps["team"] = copy.deepcopy(dict(pt_team))
            return
        merged = _odcs_team._merge_team(pt_team, then or {}, ctx.fluid)
        if isinstance(merged, Mapping):
            ctx.odps["team"] = merged
        return
    team = _odcs_team._owner_to_team(ctx.fluid)
    if team:
        ctx.odps["team"] = team


def to_fluid(ctx: ImportCtx) -> None:
    team = ctx.odps.get("team")
    if not team:
        return
    owner = _odcs_team._team_to_owner(team)
    if owner:
        ctx.fluid["owner"] = owner
    if isinstance(team, Mapping):
        metadata_passthrough(ctx.fluid)["team"] = copy.deepcopy(dict(team))
