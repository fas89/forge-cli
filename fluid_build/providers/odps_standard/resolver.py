# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Resolve Bitol ODPS port ``contractId`` references to full ODCS documents.

Resolution order (per the Phase 3 plan):

  1. **local file** — probe candidate filenames in ``base_path`` and a few
     conventional subdirs.
  2. **http(s) URL** — only when the ``contractId`` is itself a URL (or a
     ``hint`` URL is given) and ``allow_remote=True``.

Every resolved document is validated against the ODCS schema matching its
``apiVersion`` before it is returned. The resolver caches by ``contractId``
so repeated lookups inside one import pass cost nothing.

Safety rules (a ``contractId`` comes from a document the user may not trust):

- A local probe only accepts a file whose ODCS ``id`` equals the requested
  ``contractId`` (the last-segment fallback must not pick up an unrelated
  contract). Explicit hints and URLs are exempt.
- A ``contractId`` that is absolute or contains a path separator or ``..`` is
  never used as a local file stem, and every probed / indexed file must stay
  inside ``base_path`` once symlinks are resolved.
- Remote fetches follow redirects only to http(s), read at most
  :data:`DEFAULT_MAX_BYTES`, and give up after :data:`DEFAULT_TOTAL_TIMEOUT`
  seconds in total: every socket operation (connect, request, status line,
  headers, chunk-size lines, body) runs with a timeout of
  ``min(timeout, time left)``, so a server dripping its headers cannot hold
  a fetch past the deadline.
- YAML is loaded with the alias-bomb guard of
  :func:`fluid_build.providers.odcs.io.load_yaml`.
- A malformed ``contractId`` (not a string, or one with a NUL byte) is
  :class:`ContractNotFound`, never a crash, so lenient imports warn.

Errors raised:
  - :class:`ContractNotFound`        — no candidate path matched.
  - :class:`RemoteFetchDisabled`     — URL needed but ``allow_remote=False``.
  - :class:`ContractValidationError` — resolved document failed ODCS validation.
"""

from __future__ import annotations

import http.client
import io
import json
import logging
import os
import socket
import time
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse

from fluid_build.providers.base import ProviderError
from fluid_build.providers.odcs.io import load_yaml, read_input
from fluid_build.providers.odcs.provider import OdcsProvider

LOG = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10  # seconds per socket operation for http(s) fetch
DEFAULT_TOTAL_TIMEOUT = 30.0  # seconds for a whole http(s) fetch
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # largest remote contract body accepted
_READ_CHUNK = 64 * 1024
# Probing matrix sized for the common case (Bitol fragments layout: ODPS doc
# + sibling ODCS files, optionally nested under contracts/). Previously we
# probed {"", contracts, odcs, odcs/contracts} × {full-id, last-segment} × 6
# extensions = 48 candidates, which produced very noisy error messages on
# misses. Trimmed to the two most-used subdirs and the canonical
# ``.odcs.{yaml,json}`` extensions — 8 candidates total — which still covers
# every layout we've seen in the wild without burying the operator in
# tried-paths output.
DEFAULT_SUBDIRS = ("", "contracts")
DEFAULT_EXTENSIONS = (".odcs.yaml", ".odcs.yml", ".odcs.json", ".yaml")


class ContractNotFound(ProviderError):
    """No candidate path / URL matched the requested contractId."""

    _MAX_TRIED_DISPLAYED = 5

    def __init__(self, contract_id: str, tried: List[str]):
        self.contract_id = contract_id
        self.tried = list(tried)
        if tried:
            head = tried[: self._MAX_TRIED_DISPLAYED]
            tail = (
                f"\n  …and {len(tried) - self._MAX_TRIED_DISPLAYED} more"
                if len(tried) > self._MAX_TRIED_DISPLAYED
                else ""
            )
            msg = (
                f"Could not resolve contractId {contract_id!r}. Tried:\n  "
                + "\n  ".join(head)
                + tail
            )
        else:
            msg = f"Could not resolve contractId {contract_id!r} (no candidates probed)"
        super().__init__(msg)


class RemoteFetchDisabled(ProviderError):
    """A URL was given but ``allow_remote=False``."""


class ContractValidationError(ProviderError):
    """A resolved ODCS document failed schema validation."""


@dataclass
class ResolvedContract:
    """The result of a successful resolve() call."""

    odcs: Dict[str, Any]
    source: str  # "local" | "remote"
    origin: str  # the path or URL the doc was loaded from
    contract_id: str


@dataclass
class ContractResolver:
    """Lookup ODCS contracts by ``contractId``.

    Parameters
    ----------
    base_path:
        Directory to probe for local candidates. May be a file (its parent
        directory is used) or a directory.
    allow_remote:
        If ``False``, raise :class:`RemoteFetchDisabled` whenever a URL is the
        only viable candidate. Default ``True``.
    timeout:
        Socket timeout for http(s) fetches (per socket operation).
    odcs_provider:
        Optional :class:`OdcsProvider` to use for validation. A shared
        instance avoids reloading the JSON Schema for every resolve.
    candidate_extensions / candidate_subdirs:
        Override the default probing matrix.
    additional_files:
        Extra absolute paths to consider before the default probes — used by
        :meth:`BitolOdpsProvider.import_directory` so files discovered by
        directory scan get tried first.
    max_bytes / total_timeout:
        Remote fetch limits: the largest body accepted and the deadline for
        a whole fetch (connect + redirects + headers + body).
    """

    base_path: Optional[Path] = None
    allow_remote: bool = True
    timeout: int = DEFAULT_TIMEOUT
    odcs_provider: Optional[OdcsProvider] = None
    candidate_extensions: tuple = DEFAULT_EXTENSIONS
    candidate_subdirs: tuple = DEFAULT_SUBDIRS
    additional_files: List[Path] = field(default_factory=list)
    max_bytes: int = DEFAULT_MAX_BYTES
    total_timeout: float = DEFAULT_TOTAL_TIMEOUT

    _cache: Dict[str, ResolvedContract] = field(default_factory=dict, init=False)
    _index: Dict[str, Path] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.base_path is not None:
            self.base_path = Path(self.base_path)
            if self.base_path.is_file():
                self.base_path = self.base_path.parent
        if self.odcs_provider is None:
            self.odcs_provider = OdcsProvider()
        # Build a quick id→path index for any pre-discovered files
        for fpath in self.additional_files:
            self._index_file(fpath)

    # ---- public API -----------------------------------------------------

    def index_directory(self, dir_path: Union[str, Path]) -> None:
        """Pre-scan a directory tree for ``*.odcs.yaml``/``*.odcs.yml``/``*.odcs.json``
        files and index them by their ODCS ``id`` for first-hit lookup."""
        root = Path(dir_path)
        if not root.is_dir():
            return
        for child in root.rglob("*"):
            if (
                child.is_file()
                and child.suffix.lower() in (".yaml", ".yml", ".json")
                and _is_within(child, root)
            ):
                self._index_file(child)

    def resolve(
        self, contract_id: str, *, hint: Optional[Union[str, Path]] = None
    ) -> ResolvedContract:
        if not isinstance(contract_id, str) or "\x00" in contract_id:
            # Not usable as a file stem, a URL or an index key; a document
            # can carry anything here, so this is "not found", not a crash.
            raise ContractNotFound(str(contract_id), [])
        if contract_id in self._cache:
            return self._cache[contract_id]

        tried: List[str] = []

        # 0. Explicit hint (file path or URL) wins
        if hint:
            resolved = self._try_hint(contract_id, hint, tried)
            if resolved is not None:
                return self._cache_and_return(contract_id, resolved)

        # 1. Pre-indexed file (from index_directory / additional_files)
        indexed = self._index.get(contract_id)
        if indexed is not None:
            tried.append(str(indexed))
            resolved = self._load_local(contract_id, indexed)
            if resolved is not None:
                return self._cache_and_return(contract_id, resolved)

        # 2. Local file probes — only a document whose ``id`` is the
        # requested contractId counts (the last-segment fallback would
        # otherwise pick up an unrelated contract of the same port name).
        if self.base_path is not None:
            for candidate in self._local_candidates(contract_id):
                tried.append(str(candidate))
                if candidate.exists():
                    resolved = self._load_local(contract_id, candidate, require_id_match=True)
                    if resolved is not None:
                        return self._cache_and_return(contract_id, resolved)

        # 3. http(s) if the contractId itself is a URL
        if _looks_like_url(contract_id):
            if not self.allow_remote:
                raise RemoteFetchDisabled(
                    f"Cannot resolve contractId {contract_id!r}: remote fetch disabled"
                )
            tried.append(contract_id)
            resolved = self._load_remote(contract_id, contract_id)
            if resolved is not None:
                return self._cache_and_return(contract_id, resolved)

        raise ContractNotFound(contract_id, tried)

    # ---- internals ------------------------------------------------------

    def _local_candidates(self, contract_id: str) -> List[Path]:
        if self.base_path is None:
            return []
        if not _is_safe_stem(contract_id):
            # An absolute path or one with separators / ".." would escape
            # base_path (pathlib drops base_path for an absolute stem).
            LOG.debug("contractId %r is not a plain file stem; no local probe", contract_id)
            return []
        # The contractId may itself contain dots (e.g. "product.daily_orders").
        # We probe both the full id and the last segment (port name) to be lenient.
        stems = [contract_id]
        if "." in contract_id:
            stems.append(contract_id.rsplit(".", 1)[-1])
        candidates: List[Path] = []
        for sub in self.candidate_subdirs:
            sub_dir = self.base_path / sub if sub else self.base_path
            for stem in stems:
                for ext in self.candidate_extensions:
                    candidate = sub_dir / f"{stem}{ext}"
                    if _is_within(candidate, self.base_path):
                        candidates.append(candidate)
        return candidates

    def _try_hint(
        self, contract_id: str, hint: Union[str, Path], tried: List[str]
    ) -> Optional[ResolvedContract]:
        if isinstance(hint, Path) or not _looks_like_url(str(hint)):
            path = Path(hint)
            tried.append(str(path))
            if path.exists():
                return self._load_local(contract_id, path)
            return None
        # Hint is a URL
        if not self.allow_remote:
            raise RemoteFetchDisabled(
                f"Cannot resolve contractId {contract_id!r} via hint URL: remote fetch disabled"
            )
        tried.append(str(hint))
        return self._load_remote(contract_id, str(hint))

    def _index_file(self, path: Path) -> None:
        try:
            data = read_input(path)
        except Exception as exc:
            LOG.debug("Skipping unreadable file %s: %s", path, exc)
            return
        if not isinstance(data, Mapping):
            return
        odcs_id = data.get("id")
        if isinstance(odcs_id, str):
            self._index.setdefault(odcs_id, path)

    def _load_local(
        self, contract_id: str, path: Path, *, require_id_match: bool = False
    ) -> Optional[ResolvedContract]:
        try:
            data = read_input(path)
        except Exception as exc:
            LOG.warning("Failed to read %s: %s", path, exc)
            return None
        if not isinstance(data, Mapping):
            return None
        if require_id_match and data.get("id") != contract_id:
            LOG.debug("Skipping %s: id %r != contractId %r", path, data.get("id"), contract_id)
            return None
        self._validate(contract_id, data, origin=str(path))
        return ResolvedContract(
            odcs=dict(data),
            source="local",
            origin=str(path),
            contract_id=contract_id,
        )

    def _load_remote(self, contract_id: str, url: str) -> Optional[ResolvedContract]:
        import yaml

        content_type, body = self._fetch(contract_id, url)

        # Refuse HTML-with-200 — common when the URL is wrong or it returned a portal page
        if "text/html" in content_type.lower() or body.lstrip().startswith(b"<"):
            raise ContractNotFound(contract_id, [url])

        try:
            text = body.decode("utf-8", errors="strict")
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                data = load_yaml(text, source=url)
        except (UnicodeDecodeError, yaml.YAMLError, ProviderError) as exc:
            LOG.warning("Could not parse %s: %s", url, exc)
            raise ContractNotFound(contract_id, [url]) from exc
        if not isinstance(data, Mapping):
            raise ContractNotFound(contract_id, [url])
        self._validate(contract_id, data, origin=url)
        return ResolvedContract(
            odcs=dict(data),
            source="remote",
            origin=url,
            contract_id=contract_id,
        )

    def _fetch(self, contract_id: str, url: str) -> Tuple[str, bytes]:
        """GET ``url`` within the size and time limits; ``(content_type, body)``.

        Every socket operation is armed with ``min(timeout, time left)``
        (see :class:`_DeadlineSocket`), so the whole fetch (connect,
        redirects, headers, chunk-size lines and body) ends by
        ``total_timeout``.
        """
        deadline = time.monotonic() + self.total_timeout
        try:
            with urlopen(url, timeout=self.timeout, deadline=deadline) as response:
                if time.monotonic() > deadline:
                    raise ContractNotFound(contract_id, [url])
                final_url = response.geturl() if hasattr(response, "geturl") else url
                if not _looks_like_url(str(final_url or url)):
                    raise ContractNotFound(contract_id, [url])
                content_type = response.headers.get("Content-Type", "") or ""
                length = response.headers.get("Content-Length")
                if length and length.isdigit() and int(length) > self.max_bytes:
                    raise ContractNotFound(contract_id, [url])
                chunks: List[bytes] = []
                total = 0
                while True:
                    if time.monotonic() > deadline:
                        LOG.warning("Fetching %s exceeded %gs; giving up", url, self.total_timeout)
                        raise ContractNotFound(contract_id, [url])
                    want = min(_READ_CHUNK, self.max_bytes + 1 - total)
                    # ``read1`` returns after one socket read, so a slow
                    # server cannot hold a single call past the deadline.
                    chunk = response.read1(want) if hasattr(response, "read1") else None
                    if chunk is None:
                        chunk = response.read(want)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    total += len(chunk)
                    if total > self.max_bytes:
                        LOG.warning("%s is larger than %d bytes; refusing it", url, self.max_bytes)
                        raise ContractNotFound(contract_id, [url])
        except (URLError, OSError, http.client.HTTPException, ValueError) as exc:
            # URLError covers HTTP errors and refused redirects; OSError covers
            # socket timeouts / resets; ValueError a malformed URL.
            raise ContractNotFound(contract_id, [url]) from exc
        return content_type, b"".join(chunks)

    def _validate(self, contract_id: str, data: Mapping[str, Any], *, origin: str) -> None:
        if self.odcs_provider is None or not self.odcs_provider.schema:
            return
        try:
            self.odcs_provider.validate_contract(data)
        except ProviderError as exc:
            raise ContractValidationError(
                f"Resolved contract for {contract_id!r} from {origin} "
                f"failed ODCS validation: {exc}"
            ) from exc

    def _cache_and_return(self, contract_id: str, resolved: ResolvedContract) -> ResolvedContract:
        self._cache[contract_id] = resolved
        return resolved


def urlopen(url: str, *, timeout: Optional[float], deadline: Optional[float] = None) -> Any:
    """``urllib.request.urlopen`` whose redirects may only lead to http(s).

    With ``deadline`` (a :func:`time.monotonic` value), every socket
    operation of every connection the fetch opens (redirects included)
    times out at ``min(timeout, deadline - now)``; a ``None`` per-operation
    ``timeout`` leaves only the deadline.
    """
    handlers: List[Any] = [_HttpOnlyRedirectHandler()]
    if deadline is not None:
        handlers.append(_DeadlineHTTPHandler(deadline))
        if _DeadlineHTTPSHandler is not None:
            handlers.append(_DeadlineHTTPSHandler(deadline))
        remaining = max(deadline - time.monotonic(), 0.001)
        timeout = remaining if timeout is None else min(timeout, remaining)
    opener = urllib.request.build_opener(*handlers)
    return opener.open(url, timeout=timeout)


def _time_left(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise socket.timeout("remote contract fetch exceeded its total deadline")
    return remaining


class _DeadlineSocket:
    """Socket proxy that arms each operation with ``min(timeout, time left)``.

    ``http.client`` reads the status line, headers and chunk-size lines
    through ``sock.makefile("rb")``; the reader returned here goes through
    :meth:`recv_into`, so those reads are bounded by the deadline as well.
    """

    def __init__(self, sock: Any, deadline: float, timeout: Optional[float]) -> None:
        self._sock = sock
        self._deadline = deadline
        self._timeout = timeout
        self._readers = 0
        self._closed = False

    def _arm(self) -> None:
        remaining = _time_left(self._deadline)
        per_op = remaining if self._timeout is None else min(self._timeout, remaining)
        self._sock.settimeout(per_op)

    def recv_into(self, buffer: Any, nbytes: int = 0, flags: int = 0) -> int:
        self._arm()
        return self._sock.recv_into(buffer, nbytes, flags)

    def recv(self, bufsize: int, flags: int = 0) -> bytes:
        self._arm()
        return self._sock.recv(bufsize, flags)

    def send(self, data: Any, flags: int = 0) -> int:
        self._arm()
        return self._sock.send(data, flags)

    def sendall(self, data: Any, flags: int = 0) -> None:
        self._arm()
        self._sock.sendall(data, flags)

    def makefile(self, mode: str = "r", buffering: Optional[int] = None, **kwargs: Any) -> Any:
        if mode not in ("rb", "br"):
            raise ValueError(f"_DeadlineSocket.makefile supports only 'rb', not {mode!r}")
        self._readers += 1
        size = buffering if buffering and buffering > 0 else io.DEFAULT_BUFFER_SIZE
        return io.BufferedReader(_DeadlineReader(self), size)

    def _reader_closed(self) -> None:
        self._readers -= 1
        if self._closed and self._readers <= 0:
            self._sock.close()

    def close(self) -> None:
        # Like ``socket.socket``: the descriptor stays open while a reader
        # made by ``makefile`` is (urllib closes the connection's socket
        # right after reading the headers and keeps reading the body).
        self._closed = True
        if self._readers <= 0:
            self._sock.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._sock, name)


class _DeadlineReader(io.RawIOBase):
    """Raw binary reader over a :class:`_DeadlineSocket`."""

    def __init__(self, sock: _DeadlineSocket) -> None:
        super().__init__()
        self._dsock = sock

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        return self._dsock.recv_into(buffer)

    def close(self) -> None:
        if not self.closed:
            super().close()
            self._dsock._reader_closed()


class _DeadlineHandlerMixin:
    """Gives every connection an urllib handler opens a deadline-armed socket."""

    _deadline: float

    def do_open(self, http_class: Any, req: Any, **http_conn_args: Any) -> Any:
        deadline = self._deadline

        def connection(*args: Any, **kwargs: Any) -> Any:
            conn = http_class(*args, **kwargs)
            plain_connect = conn.connect
            per_op = conn.timeout if isinstance(conn.timeout, (int, float)) else None

            def connect() -> None:
                remaining = _time_left(deadline)
                conn.timeout = remaining if per_op is None else min(per_op, remaining)
                plain_connect()
                conn.sock = _DeadlineSocket(conn.sock, deadline, per_op)

            conn.connect = connect
            return conn

        return super().do_open(connection, req, **http_conn_args)  # type: ignore[misc]


class _DeadlineHTTPHandler(_DeadlineHandlerMixin, urllib.request.HTTPHandler):
    def __init__(self, deadline: float) -> None:
        super().__init__()
        self._deadline = deadline


_DeadlineHTTPSHandler: Optional[type] = None
if hasattr(urllib.request, "HTTPSHandler"):  # Python built with ssl

    class _DeadlineHTTPSHandlerImpl(_DeadlineHandlerMixin, urllib.request.HTTPSHandler):
        def __init__(self, deadline: float) -> None:
            super().__init__()
            self._deadline = deadline

    _DeadlineHTTPSHandler = _DeadlineHTTPSHandlerImpl


class _HttpOnlyRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow redirects only to http(s) (urllib's default also allows ftp)."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        target = urljoin(req.full_url, newurl)
        if urlparse(target).scheme.lower() not in ("http", "https"):
            raise HTTPError(
                req.full_url,
                code,
                f"refusing redirect to non-http(s) URL {target!r}",
                headers,
                fp,
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _is_safe_stem(contract_id: str) -> bool:
    """A contractId usable as a local file stem: relative, no separators, no ``..``."""
    if not isinstance(contract_id, str) or not contract_id or ".." in contract_id:
        return False
    if "\x00" in contract_id:
        return False
    if "/" in contract_id or "\\" in contract_id or os.sep in contract_id:
        return False
    if os.altsep is not None and os.altsep in contract_id:
        return False
    return not (os.path.isabs(contract_id) or Path(contract_id).drive)


def _is_within(path: Path, root: Path) -> bool:
    """True when ``path`` (symlinks resolved) stays inside ``root``."""
    try:
        return path.resolve().is_relative_to(Path(root).resolve())
    except (OSError, RuntimeError, ValueError):
        # ValueError: an embedded NUL byte.
        return False


def _looks_like_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
    except ValueError:  # e.g. "http://[" (invalid IPv6 literal)
        return False
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)
