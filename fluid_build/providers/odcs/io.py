# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Filesystem helpers for the ODCS provider — read YAML/JSON, write YAML/JSON.

Contracts are often untrusted (a bundle someone sent, a remote
``contractId``), so YAML is loaded through :func:`load_yaml`, which refuses
documents whose aliases would expand into an enormous tree ("billion
laughs"): PyYAML shares aliased objects, but validation, deep copies and
every mapper walk the *expanded* tree, so a few hundred bytes could cost
minutes and gigabytes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import IO, Any, Dict, List, Optional, Tuple, Union

from fluid_build.providers.base import ProviderError

# Nodes alias expansion may add on top of a document's own nodes. Real
# contracts use aliases sparingly (if at all); the cap only stops documents
# whose expanded size is out of all proportion to what was written.
MAX_ALIAS_EXPANSION = 1_000_000


def read_input(path: Union[str, Path]) -> Dict[str, Any]:
    input_path = Path(path)
    with open(input_path) as f:
        if input_path.suffix in (".yaml", ".yml"):
            return load_yaml(f, source=str(input_path))
        return json.load(f)


def load_yaml(stream: Union[str, bytes, IO[Any]], *, source: Optional[str] = None) -> Any:
    """``yaml.safe_load`` that refuses alias bombs.

    The document is composed into a node graph first (aliases are shared
    nodes there, so this is linear in the input) and its *expanded* node
    count is computed without expanding anything. A document whose aliases
    would add more than :data:`MAX_ALIAS_EXPANSION` nodes, or that is
    recursive, raises :class:`ProviderError` before any Python object is
    built.
    """
    import yaml

    loader = yaml.SafeLoader(stream)
    try:
        node = loader.get_single_node()
        if node is None:
            return None
        _check_alias_expansion(node, source)
        return loader.construct_document(node)
    finally:
        loader.dispose()


def _check_alias_expansion(root: Any, source: Optional[str]) -> None:
    import yaml

    where = f" in {source}" if source else ""
    sizes: Dict[int, int] = {}  # id(node) -> expanded size of its subtree
    in_progress = set()
    # Iterative post-order walk: (node, children-already-pushed).
    stack: List[Tuple[Any, bool]] = [(root, False)]
    unique = 0
    while stack:
        node, expanded = stack.pop()
        key = id(node)
        if not expanded:
            if key in sizes:
                continue
            if key in in_progress:
                raise ProviderError(f"Refusing recursive YAML alias{where}")
            in_progress.add(key)
            unique += 1
            stack.append((node, True))
            for child in _children(node, yaml):
                if id(child) in in_progress:
                    raise ProviderError(f"Refusing recursive YAML alias{where}")
                if id(child) not in sizes:
                    stack.append((child, False))
            continue
        total = 1 + sum(sizes[id(child)] for child in _children(node, yaml))
        sizes[key] = total
        in_progress.discard(key)
        if total - unique > MAX_ALIAS_EXPANSION:
            raise ProviderError(
                f"Refusing YAML document{where}: its aliases expand to more than "
                f"{MAX_ALIAS_EXPANSION:,} extra nodes (possible alias bomb)"
            )


def _children(node: Any, yaml: Any) -> List[Any]:
    if isinstance(node, yaml.SequenceNode):
        return list(node.value)
    if isinstance(node, yaml.MappingNode):
        out: List[Any] = []
        for key_node, value_node in node.value:
            out.append(key_node)
            out.append(value_node)
        return out
    return []


def write_output(data: Dict[str, Any], path: Union[Path, str], fmt: str) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        if fmt == "yaml":
            import yaml

            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        else:
            json.dump(data, f, indent=2)
