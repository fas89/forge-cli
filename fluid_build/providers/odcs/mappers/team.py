# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Team ↔ Owner mapping.

ODCS v3.1.0 ``team`` is an **object**:
``{id, name, description, members[], tags, customProperties, authoritativeDefinitions}``.
Each ``members[]`` entry has a required ``username`` (often an email) plus
optional ``name``, ``role``, ``dateIn``, ``dateOut``, ``replacedByUsername``,
``description``, ``tags``, ``customProperties``, ``authoritativeDefinitions``.

The v3.0.x array-of-members form (deprecated, still accepted by v3.1.0) is
read the same way; a legacy string form is accepted on import (best-effort).

For lossless round-trip the verbatim team (object or array) is preserved
under ``metadata.odcs_passthrough.team`` and replayed unless the FLUID
``owner`` changed since import; an edit is merged onto it, keeping every
member key FLUID does not model.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from .base import (
    ExportCtx,
    ImportCtx,
    get_metadata_passthrough,
    metadata_passthrough,
)

# ----- ODCS → FLUID --------------------------------------------------------


def to_fluid(ctx: ImportCtx) -> None:
    team = ctx.odcs.get("team")
    if team is None:
        return
    owner = _import_owner(team)
    if owner:
        ctx.fluid["owner"] = owner
    if isinstance(team, (Mapping, list)):
        # Verbatim, whatever its shape — including the deprecated v3.0.x
        # array-of-members form and an explicit empty ``{}`` / ``[]``.
        metadata_passthrough(ctx.fluid)["team"] = copy.deepcopy(team)


def _import_owner(team: Any) -> Optional[Dict[str, Any]]:
    """The FLUID ``owner`` an import projects from ``team`` (``None`` if empty)."""
    if not team:
        return None
    return _team_to_owner(team)


def _team_to_owner(team: Any) -> Optional[Dict[str, Any]]:
    if isinstance(team, str):
        return {"team": team, "name": team}
    if isinstance(team, list):
        # ODCS v3.0.x ``team`` is an array of members (deprecated in v3.1.0).
        team = {"members": team}
    if not isinstance(team, Mapping):
        return None

    owner: Dict[str, Any] = {}
    team_name = team.get("name")
    if team_name:
        owner["team"] = team_name

    members = team.get("members") or []
    if not isinstance(members, list):
        members = []

    first = members[0] if members else None
    rest = members[1:] if members else []

    if isinstance(first, Mapping):
        if first.get("name"):
            owner["name"] = first["name"]
        if first.get("username"):
            owner["email"] = first["username"]
        if first.get("role"):
            owner["role"] = first["role"]

    contacts: List[Dict[str, Any]] = []
    for member in rest:
        if not isinstance(member, Mapping):
            continue
        contact: Dict[str, Any] = {}
        if member.get("name"):
            contact["name"] = member["name"]
        if member.get("username"):
            contact["email"] = member["username"]
        if member.get("role"):
            contact["role"] = member["role"]
        if contact:
            contacts.append(contact)
    if contacts:
        owner["contacts"] = contacts

    if not owner:
        owner["team"] = team_name or "unknown"
    return owner


# ----- FLUID → ODCS --------------------------------------------------------


def _current_owner(fluid: Mapping[str, Any]) -> Any:
    return fluid.get("owner") or (fluid.get("metadata") or {}).get("owner") or None


def to_odcs(ctx: ExportCtx) -> None:
    pt = get_metadata_passthrough(ctx.fluid)
    pt_team = pt.get("team")
    if isinstance(pt_team, (Mapping, list)):
        # Imported contract: three-way merge, like metadata and schema. The
        # stashed team is replayed verbatim unless the FLUID owner changed.
        then = _import_owner(pt_team)
        now = _current_owner(ctx.fluid)
        if now == then:
            ctx.odcs["team"] = copy.deepcopy(pt_team)
            return
        merged = _merge_team(pt_team, then or {}, ctx.fluid)
        if merged is not None:
            ctx.odcs["team"] = merged
        return

    team = _owner_to_team(ctx.fluid)
    if team:
        ctx.odcs["team"] = team


# TeamMember keys FLUID models (owner/contacts name, email, role).
_MEMBER_NATIVE_KEYS = ("name", "username", "role")


def _merge_team(stashed: Any, then: Mapping[str, Any], fluid: Mapping[str, Any]) -> Any:
    """Apply a FLUID owner edit to the stashed team, keeping unmodelled keys.

    Only a removed owner removes the team. A stashed team without a name
    (the v3.0.x array form, or an object without ``name``) keeps its shape
    and stays nameless unless the owner's ``team`` is set.
    """
    now = _current_owner(fluid)
    if not isinstance(now, Mapping) or not now:
        return None  # owner removed

    stashed_members = stashed.get("members") if isinstance(stashed, Mapping) else stashed
    stashed_members = [m for m in stashed_members or [] if isinstance(m, Mapping)]
    member_keys = ("name", "email", "role", "contacts")
    if any(now.get(k) != then.get(k) for k in member_keys):
        members = _merge_members(stashed_members, _owner_members(now))
    else:
        members = copy.deepcopy(stashed_members)

    team_changed = now.get("team") != then.get("team")
    if isinstance(stashed, list) and not team_changed:
        return members  # keep the array form when only members changed

    out = copy.deepcopy(dict(stashed)) if isinstance(stashed, Mapping) else {}
    if team_changed:
        name = now.get("team") or now.get("name")
        if name:
            out["name"] = name
        else:
            out.pop("name", None)
    if members:
        out["members"] = members
    else:
        out.pop("members", None)
    return out


def _merge_members(
    stashed: List[Mapping[str, Any]], fresh: List[Mapping[str, Any]]
) -> List[Dict[str, Any]]:
    """Fresh members, each keeping the stashed extras (dateIn, tags, ...) of
    the stashed member with the same ``username`` (or ``name``) — or, when
    none matches, of the stashed member at the same position whose own key
    no longer appears (its username was edited)."""

    def key_of(m: Mapping[str, Any]) -> Any:
        return m.get("username") or m.get("name")

    fresh_keys = {key_of(m) for m in fresh}
    out: List[Dict[str, Any]] = []
    used = set()
    for pos, member in enumerate(fresh):
        key = key_of(member)
        match = None
        for i, candidate in enumerate(stashed):
            if i not in used and key and key_of(candidate) == key:
                match = i
                break
        if (
            match is None
            and pos < len(stashed)
            and pos not in used
            and key_of(stashed[pos]) not in fresh_keys
        ):
            match = pos
        merged: Dict[str, Any] = {}
        if match is not None:
            used.add(match)
            merged = {
                k: copy.deepcopy(v)
                for k, v in stashed[match].items()
                if k not in _MEMBER_NATIVE_KEYS
            }
        merged.update(copy.deepcopy(dict(member)))
        out.append(merged)
    return out


def _owner_to_team(fluid: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    owner = fluid.get("owner") or (fluid.get("metadata") or {}).get("owner") or {}
    if not isinstance(owner, Mapping):
        owner = {}

    team_name = owner.get("team") or owner.get("name")
    if not team_name:
        return None

    team_obj: Dict[str, Any] = {"name": team_name}
    members = _owner_members(owner)
    if members:
        team_obj["members"] = members
    return team_obj


def _owner_members(owner: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """ODCS ``TeamMember`` entries for a FLUID owner (primary, then contacts)."""
    members: List[Dict[str, Any]] = []

    # Primary contact from owner top-level
    if owner.get("name") or owner.get("email"):
        member: Dict[str, Any] = {}
        if owner.get("name"):
            member["name"] = owner["name"]
        if owner.get("email"):
            # ODCS TeamMember.username is the email-or-username slot
            member["username"] = owner["email"]
        if owner.get("role"):
            member["role"] = owner["role"]
        members.append(member)

    # Additional contacts
    for contact in owner.get("contacts") or []:
        if not isinstance(contact, Mapping):
            continue
        member = {}
        if contact.get("name"):
            member["name"] = contact["name"]
        if contact.get("email"):
            member["username"] = contact["email"]
        if contact.get("role"):
            member["role"] = contact["role"]
        if member:
            members.append(member)
    return members
