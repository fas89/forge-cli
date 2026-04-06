# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""LLM provider adapters and configuration for the forge copilot."""

from __future__ import annotations

__all__ = [
    "CopilotGenerationError",
    "LlmConfig",
    "LlmReadinessCheck",
    "LlmProvider",
    "OpenAIProvider",
    "OllamaProvider",
    "AnthropicProvider",
    "GeminiProvider",
    "BUILTIN_LLM_PROVIDERS",
    "PROVIDER_DISPLAY_NAMES",
    "check_llm_readiness",
    "clear_api_key_from_keyring",
    "get_catalog_default",
    "detect_ollama_available",
    "detect_provider_from_api_key",
    "get_llm_provider",
    "normalize_llm_provider_name",
    "query_ollama_models",
    "reset_llm_caches",
    "resolve_llm_config",
    "resolve_model_name",
    "resolve_ollama_model",
    "save_api_key_to_keyring",
    "call_llm",
]

import json
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import httpx

from fluid_build.cli._common import CLIError

LOG = logging.getLogger("fluid.cli.forge_copilot.llm")


# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------


class CopilotGenerationError(CLIError):
    """Structured error for copilot generation failures."""

    def __init__(
        self,
        event: str,
        message: str,
        suggestions: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        payload = {"message": message}
        if context:
            payload.update(context)
        super().__init__(1, event, payload)
        self.message = message
        self.suggestions = suggestions or []


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class LlmConfig:
    """Resolved configuration for a provider-backed LLM call."""

    provider: str
    model: str
    endpoint: str
    api_key: Optional[str]
    timeout_seconds: int = 120

    @property
    def redacted_endpoint(self) -> str:
        endpoint = self.endpoint
        # Redact common credential query parameters
        endpoint = re.sub(
            r"([?&](?:key|token|api_key|auth|secret|credential|password)=)[^&]+",
            r"\1***",
            endpoint,
            flags=re.I,
        )
        # Redact userinfo in URLs (user:pass@host)
        endpoint = re.sub(r"(https?://)([^@/]+)@", r"\1***:***@", endpoint)
        return endpoint


@dataclass
class LlmReadinessCheck:
    """Lightweight preflight state for interactive copilot onboarding."""

    ready: bool
    provider: str
    model: str
    endpoint: str
    auth_available: bool
    error: Optional[CopilotGenerationError] = None


# ---------------------------------------------------------------------------
# Provider Interface & Implementations
# ---------------------------------------------------------------------------


class LlmProvider(ABC):
    """Interface for provider-specific request/response translation."""

    name: str
    default_model: str

    @abstractmethod
    def default_endpoint(self, model: str, env: Mapping[str, str]) -> str:
        """Return the provider's default endpoint for the resolved model."""

    @abstractmethod
    def build_request(
        self, config: LlmConfig, system_prompt: str, user_prompt: str
    ) -> tuple[Dict[str, str], Dict[str, Any]]:
        """Build request headers and JSON payload."""

    @abstractmethod
    def extract_text(self, response_json: Dict[str, Any]) -> str:
        """Extract free-form response text from the provider response."""


class OpenAIProvider(LlmProvider):
    name = "openai"
    default_model = "gpt-4o-mini"

    def default_endpoint(self, model: str, env: Mapping[str, str]) -> str:
        base = env.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
        if base != "https://api.openai.com/v1":
            LOG.warning(
                "OPENAI_BASE_URL is set to a non-default value (%s). "
                "Your API key will be sent to this host.",
                base,
            )
        return base + "/chat/completions"

    def build_request(
        self, config: LlmConfig, system_prompt: str, user_prompt: str
    ) -> tuple[Dict[str, str], Dict[str, Any]]:
        headers = {"Content-Type": "application/json"}
        if config.api_key:
            headers["Authorization"] = f"Bearer {config.api_key}"
        payload = {
            "model": config.model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        return headers, payload

    def extract_text(self, response_json: Dict[str, Any]) -> str:
        return response_json["choices"][0]["message"]["content"]


class OllamaProvider(OpenAIProvider):
    name = "ollama"
    default_model = "llama3.1"

    def default_endpoint(self, model: str, env: Mapping[str, str]) -> str:
        host = env.get("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
        return host + "/v1/chat/completions"

    def build_request(
        self, config: LlmConfig, system_prompt: str, user_prompt: str
    ) -> tuple[Dict[str, str], Dict[str, Any]]:
        headers, payload = super().build_request(config, system_prompt, user_prompt)
        headers.pop("Authorization", None)
        return headers, payload


class AnthropicProvider(LlmProvider):
    name = "anthropic"
    default_model = "claude-3-5-sonnet-latest"

    def default_endpoint(self, model: str, env: Mapping[str, str]) -> str:
        return "https://api.anthropic.com/v1/messages"

    def build_request(
        self, config: LlmConfig, system_prompt: str, user_prompt: str
    ) -> tuple[Dict[str, str], Dict[str, Any]]:
        headers = {
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        }
        if config.api_key:
            headers["x-api-key"] = config.api_key
        payload = {
            "model": config.model,
            "max_tokens": 8192,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        return headers, payload

    def extract_text(self, response_json: Dict[str, Any]) -> str:
        content = response_json.get("content") or []
        for part in content:
            if part.get("type") == "text":
                return part.get("text", "")
        raise KeyError("Anthropic response did not contain a text block")


class GeminiProvider(LlmProvider):
    name = "gemini"
    default_model = "gemini-2.5-flash"

    def default_endpoint(self, model: str, env: Mapping[str, str]) -> str:
        safe_model = _sanitize_model_for_url(model)
        return (
            f"https://generativelanguage.googleapis.com/v1beta/models/{safe_model}:generateContent"
        )

    def build_request(
        self, config: LlmConfig, system_prompt: str, user_prompt: str
    ) -> tuple[Dict[str, str], Dict[str, Any]]:
        headers = {"Content-Type": "application/json"}
        if config.api_key:
            headers["x-goog-api-key"] = config.api_key
        payload = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
            "generationConfig": {"temperature": 0.2},
        }
        return headers, payload

    def extract_text(self, response_json: Dict[str, Any]) -> str:
        candidates = response_json.get("candidates") or []
        for candidate in candidates:
            content = candidate.get("content") or {}
            for part in content.get("parts") or []:
                text = part.get("text")
                if text:
                    return text
        raise KeyError("Gemini response did not contain any text")


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

BUILTIN_LLM_PROVIDERS: Dict[str, LlmProvider] = {
    "openai": OpenAIProvider(),
    "anthropic": AnthropicProvider(),
    "claude": AnthropicProvider(),
    "gemini": GeminiProvider(),
    "ollama": OllamaProvider(),
}


def normalize_llm_provider_name(value: Any) -> str:
    """Normalize LLM provider aliases (openai, anthropic, gemini, ollama).

    Unlike ``normalize_provider_name`` in ``forge_copilot_runtime`` (which
    handles infrastructure providers like gcp/aws/local), this function
    understands LLM-specific aliases such as ``"claude"`` → ``"anthropic"``.
    """
    if value is None:
        return "openai"
    normalized = str(value).strip().lower().replace("-", "_")
    if normalized == "claude":
        return "anthropic"
    return normalized


def get_llm_provider(name: str) -> LlmProvider:
    """Resolve a provider adapter by name."""
    normalized = (name or "").strip().lower()
    provider = BUILTIN_LLM_PROVIDERS.get(normalized)
    if not provider:
        raise CopilotGenerationError(
            "copilot_invalid_llm_provider",
            f"Unsupported LLM provider '{name}'.",
            suggestions=[
                "Choose one of: openai, anthropic, gemini, ollama",
                "Use --llm-provider or FLUID_LLM_PROVIDER to select a provider",
            ],
        )
    return provider


# ---------------------------------------------------------------------------
# Config Resolution
# ---------------------------------------------------------------------------


def resolve_llm_config(args: Any, environ: Optional[Mapping[str, str]] = None) -> LlmConfig:
    """Resolve provider, model, endpoint, and API key from flags and env vars."""
    env = dict(environ or os.environ)
    provider_name = (
        getattr(args, "llm_provider", None)
        or env.get("FLUID_LLM_PROVIDER")
        or _infer_provider_from_env(env)
        or "openai"
    )
    provider = get_llm_provider(provider_name)

    # Resolve model: explicit flag → env var → catalog default → class default.
    catalog_default = get_catalog_default(provider.name)
    explicit_model = getattr(args, "llm_model", None) or env.get("FLUID_LLM_MODEL")
    if explicit_model:
        model = resolve_model_name(provider.name, explicit_model)
    elif provider.name == "ollama":
        model = resolve_ollama_model(env)
    else:
        model = catalog_default or provider.default_model

    if not model:
        raise CopilotGenerationError(
            "copilot_missing_llm_model",
            "No LLM model was configured for forge copilot.",
            suggestions=[
                "Set FLUID_LLM_MODEL before running fluid forge --mode copilot",
                "Or pass --llm-model on the command line",
            ],
        )

    endpoint = getattr(args, "llm_endpoint", None) or env.get("FLUID_LLM_ENDPOINT")
    if not endpoint:
        endpoint = provider.default_endpoint(model, env)
    else:
        _warn_custom_endpoint(endpoint, provider.name)

    api_key = _resolve_api_key(provider.name, env)
    if provider.name != "ollama" and not api_key:
        raise CopilotGenerationError(
            "copilot_missing_llm_api_key",
            f"No API key was configured for the {provider.name} copilot adapter.",
            suggestions=[
                "Set FLUID_LLM_API_KEY or the provider-specific API key environment variable",
                "Examples: OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, GOOGLE_API_KEY",
                "For local models, use --llm-provider ollama and optionally --llm-endpoint",
            ],
        )

    return LlmConfig(provider=provider.name, model=model, endpoint=endpoint, api_key=api_key)


def check_llm_readiness(
    args: Any,
    environ: Optional[Mapping[str, str]] = None,
) -> LlmReadinessCheck:
    """Resolve copilot readiness without starting the full runtime."""
    env = dict(environ or os.environ)
    provider_name = (
        getattr(args, "llm_provider", None)
        or env.get("FLUID_LLM_PROVIDER")
        or _infer_provider_from_env(env)
        or "openai"
    )

    try:
        provider = get_llm_provider(provider_name)
    except CopilotGenerationError as exc:
        model = str(getattr(args, "llm_model", None) or env.get("FLUID_LLM_MODEL") or "")
        endpoint = str(getattr(args, "llm_endpoint", None) or env.get("FLUID_LLM_ENDPOINT") or "")
        return LlmReadinessCheck(
            ready=False,
            provider=str(provider_name or ""),
            model=model,
            endpoint=_redact_endpoint_text(endpoint),
            auth_available=False,
            error=exc,
        )

    model = str(
        getattr(args, "llm_model", None) or env.get("FLUID_LLM_MODEL") or provider.default_model
    )
    endpoint = getattr(args, "llm_endpoint", None) or env.get("FLUID_LLM_ENDPOINT")
    if not endpoint:
        endpoint = provider.default_endpoint(model, env)
    api_key = _resolve_api_key(provider.name, env)
    auth_available = provider.name == "ollama" or bool(api_key)

    try:
        config = resolve_llm_config(args, environ=env)
    except CopilotGenerationError as exc:
        return LlmReadinessCheck(
            ready=False,
            provider=provider.name,
            model=model,
            endpoint=_redact_endpoint_text(endpoint),
            auth_available=auth_available,
            error=exc,
        )

    return LlmReadinessCheck(
        ready=True,
        provider=config.provider,
        model=config.model,
        endpoint=config.redacted_endpoint,
        auth_available=auth_available,
    )


# ---------------------------------------------------------------------------
# LLM Call with Retry
# ---------------------------------------------------------------------------

_TRANSIENT_STATUS_CODES = {429, 502, 503, 504}
_LLM_MAX_RETRIES = 2
_LLM_RETRY_BASE_SECONDS = 2.0


def call_llm(
    provider: LlmProvider,
    config: LlmConfig,
    system_prompt: str,
    user_prompt: str,
) -> str:
    """Call the configured provider and return free-form response text."""
    headers, payload = provider.build_request(config, system_prompt, user_prompt)
    last_exc: Optional[Exception] = None

    _LLM_REQUEST_SUGGESTIONS = [
        "Check the selected model and endpoint are correct",
        "Verify the API key environment variable is set",
        "Use --llm-endpoint only when you need to override the provider default",
    ]

    for attempt in range(_LLM_MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=config.timeout_seconds) as client:
                response = client.post(config.endpoint, headers=headers, json=payload)
                response.raise_for_status()
            break
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in _TRANSIENT_STATUS_CODES and attempt < _LLM_MAX_RETRIES:
                delay = _LLM_RETRY_BASE_SECONDS * (2**attempt)
                LOG.info(
                    "LLM request returned %s, retrying in %.1fs", exc.response.status_code, delay
                )
                time.sleep(delay)
                continue
            raise CopilotGenerationError(
                "copilot_llm_request_failed",
                f"LLM request failed for provider {config.provider}: {exc}",
                suggestions=_LLM_REQUEST_SUGGESTIONS,
            ) from exc
        except httpx.HTTPError as exc:
            raise CopilotGenerationError(
                "copilot_llm_network_error",
                f"LLM network error for provider {config.provider}: {exc}",
                suggestions=_LLM_REQUEST_SUGGESTIONS,
            ) from exc

    try:
        return provider.extract_text(response.json())
    except Exception as exc:  # noqa: BLE001
        raise CopilotGenerationError(
            "copilot_llm_response_invalid",
            f"LLM response from {config.provider} could not be parsed.",
            suggestions=[
                "Verify the selected model supports JSON-friendly instruction following",
                "Try a different --llm-model or --llm-provider",
            ],
        ) from exc


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

_KNOWN_LLM_HOSTS = {
    "api.openai.com",
    "api.anthropic.com",
    "generativelanguage.googleapis.com",
    "localhost",
    "127.0.0.1",
    "[::1]",
}


def _warn_custom_endpoint(endpoint: str, provider: str) -> None:
    """Log a warning when the LLM endpoint is not a known-good provider host."""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(endpoint)
        host = (parsed.hostname or "").lower()
        if parsed.scheme not in ("http", "https"):
            LOG.warning(
                "LLM endpoint uses non-HTTP scheme (%s). Your API key will be sent there.",
                endpoint,
            )
        elif host and host not in _KNOWN_LLM_HOSTS and not host.startswith("localhost"):
            LOG.warning(
                "LLM endpoint (%s) is not a recognised %s host. "
                "Your API key will be sent there.",
                endpoint,
                provider,
            )
    except Exception:  # noqa: BLE001
        pass


_SAFE_MODEL_RE = re.compile(r"^[a-zA-Z0-9._:/-]+$")


def _sanitize_model_for_url(model: str) -> str:
    """Reject model names that could cause path traversal in URL interpolation."""
    if not model or not _SAFE_MODEL_RE.match(model) or ".." in model:
        raise CopilotGenerationError(
            "copilot_invalid_model_name",
            f"Model name contains unsafe characters: {model!r}",
            suggestions=["Use a model name like 'gemini-2.5-flash' or 'gpt-4o'"],
        )
    return model


def _infer_provider_from_env(env: Mapping[str, str]) -> Optional[str]:
    detected = []
    if env.get("OPENAI_API_KEY"):
        detected.append("openai")
    if env.get("ANTHROPIC_API_KEY"):
        detected.append("anthropic")
    if env.get("GEMINI_API_KEY") or env.get("GOOGLE_API_KEY"):
        detected.append("gemini")
    if env.get("OLLAMA_HOST") or detect_ollama_available(env):
        detected.append("ollama")
    if len(detected) == 1:
        return detected[0]
    if not detected:
        # No env vars found — check the keyring for any saved provider key.
        return _infer_provider_from_keyring()
    return None


def _infer_provider_from_keyring() -> Optional[str]:
    """Return the provider name if exactly one has a saved keyring key."""
    detected = []
    for name in ("openai", "anthropic", "gemini"):
        if _get_api_key_from_keyring(name):
            detected.append(name)
    if len(detected) == 1:
        return detected[0]
    return None


def _resolve_api_key(provider: str, env: Mapping[str, str]) -> Optional[str]:
    if env.get("FLUID_LLM_API_KEY"):
        return env["FLUID_LLM_API_KEY"]
    if provider == "openai":
        key = env.get("OPENAI_API_KEY")
        if key:
            return key
    elif provider == "anthropic":
        key = env.get("ANTHROPIC_API_KEY")
        if key:
            return key
    elif provider == "gemini":
        key = env.get("GEMINI_API_KEY") or env.get("GOOGLE_API_KEY")
        if key:
            return key
    # Fallback: check the OS keyring for a saved key.
    return _get_api_key_from_keyring(provider)


# ---------------------------------------------------------------------------
# Keyring helpers
# ---------------------------------------------------------------------------

_LLM_KEYRING_PREFIX = "llm"


def _keyring_key(provider: str) -> str:
    return f"{_LLM_KEYRING_PREFIX}.{provider}.api_key"


def _get_api_key_from_keyring(provider: str) -> Optional[str]:
    """Retrieve a saved LLM API key from the OS keyring."""
    try:
        from fluid_build.credentials.keyring_store import KeyringCredentialStore

        return KeyringCredentialStore.get_credential(_keyring_key(provider))
    except Exception:  # noqa: BLE001
        LOG.debug("Keyring read failed for %s", _keyring_key(provider))
        return None


def save_api_key_to_keyring(provider: str, api_key: str) -> bool:
    """Persist an LLM API key in the OS keyring for future runs."""
    try:
        from fluid_build.credentials.keyring_store import KeyringCredentialStore

        KeyringCredentialStore.set_credential(_keyring_key(provider), api_key)
        return True
    except Exception:  # noqa: BLE001
        LOG.debug("Keyring write failed for %s", _keyring_key(provider))
        return False


def clear_api_key_from_keyring(provider: str) -> bool:
    """Remove a saved LLM API key from the OS keyring."""
    try:
        from fluid_build.credentials.keyring_store import KeyringCredentialStore

        KeyringCredentialStore.delete_credential(_keyring_key(provider))
        return True
    except Exception:  # noqa: BLE001
        LOG.debug("Keyring delete failed for %s", _keyring_key(provider))
        return False


def reset_llm_caches() -> None:
    """Clear per-process caches so detection and catalog are re-evaluated."""
    global _ollama_available_cache, _model_catalog_cache  # noqa: PLW0603
    _ollama_available_cache = None
    _model_catalog_cache = None


def _redact_endpoint_text(endpoint: Any) -> str:
    if not endpoint:
        return ""
    return LlmConfig(provider="", model="", endpoint=str(endpoint), api_key=None).redacted_endpoint


# ---------------------------------------------------------------------------
# API Key Detection
# ---------------------------------------------------------------------------

PROVIDER_DISPLAY_NAMES = {
    "openai": "OpenAI",
    "anthropic": "Anthropic (Claude)",
    "gemini": "Google Gemini",
    "ollama": "Ollama",
}


def detect_provider_from_api_key(api_key: str) -> Optional[str]:
    """Detect the LLM provider from an API key's format.

    Returns the provider name (``"openai"``, ``"anthropic"``, ``"gemini"``)
    or ``None`` if the format is not recognised.
    """
    key = (api_key or "").strip()
    if not key:
        return None
    # Anthropic keys start with sk-ant- — check before the generic sk- prefix.
    if key.startswith("sk-ant-"):
        return "anthropic"
    if key.startswith("sk-"):
        return "openai"
    if key.startswith("AIza") and 35 <= len(key) <= 45:
        return "gemini"
    return None


# ---------------------------------------------------------------------------
# Model Catalog
# ---------------------------------------------------------------------------

_model_catalog_cache: Optional[Dict[str, Any]] = None


def _load_model_catalog() -> Dict[str, Any]:
    """Load the bundled ``llm_models.json`` catalog (cached after first call)."""
    global _model_catalog_cache  # noqa: PLW0603
    if _model_catalog_cache is not None:
        return _model_catalog_cache
    catalog_path = Path(__file__).with_name("llm_models.json")
    try:
        _model_catalog_cache = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Could not load model catalog %s: %s", catalog_path, exc)
        _model_catalog_cache = {}
    return _model_catalog_cache


def get_catalog_default(provider: str) -> Optional[str]:
    """Return the catalog's default model for *provider*, or ``None``."""
    catalog = _load_model_catalog()
    entry = catalog.get("providers", {}).get(provider)
    if entry:
        return entry.get("default")
    return None


def resolve_model_name(provider: str, user_input: str) -> str:
    """Resolve a potentially fuzzy model name to its canonical id.

    Checks the bundled catalog for exact id matches and aliases.
    Returns *user_input* unchanged if no match is found (the API will
    decide whether it is valid).
    """
    text = (user_input or "").strip()
    if not text:
        return text
    catalog = _load_model_catalog()
    models = catalog.get("providers", {}).get(provider, {}).get("models") or []
    lower = text.lower()
    for entry in models:
        if lower == entry["id"].lower():
            return entry["id"]
        for alias in entry.get("aliases") or []:
            if lower == alias.lower():
                return entry["id"]
    return text


# ---------------------------------------------------------------------------
# Ollama Auto-Detection
# ---------------------------------------------------------------------------


_LOCALHOST_PREFIXES = (
    "http://localhost",
    "http://127.0.0.1",
    "http://[::1]",
    "http://0.0.0.0",
)

_ollama_available_cache: Optional[bool] = None


def _ollama_host(env: Mapping[str, str]) -> str:
    host = (env.get("OLLAMA_HOST") or "http://localhost:11434").rstrip("/")
    # SSRF guard: only allow localhost targets for Ollama.
    if not any(host.lower().startswith(prefix) for prefix in _LOCALHOST_PREFIXES):
        LOG.warning("OLLAMA_HOST points to a non-localhost address (%s), ignoring.", host)
        return "http://localhost:11434"
    return host


def detect_ollama_available(env: Mapping[str, str]) -> bool:
    """Return ``True`` if a local Ollama instance is reachable (cached per-process)."""
    global _ollama_available_cache  # noqa: PLW0603
    if _ollama_available_cache is not None:
        return _ollama_available_cache
    try:
        resp = httpx.get(f"{_ollama_host(env)}/api/version", timeout=1.0)
        _ollama_available_cache = resp.status_code == 200
    except Exception:  # noqa: BLE001
        _ollama_available_cache = False
    return _ollama_available_cache


def _parse_param_size(value: str) -> float:
    """Parse an Ollama ``parameter_size`` string like ``'32.8B'`` into a float."""
    text = (value or "").strip().upper()
    match = re.match(r"^([\d.]+)\s*([BM]?)$", text)
    if not match:
        return 0.0
    number = float(match.group(1))
    unit = match.group(2)
    if unit == "M":
        return number / 1000.0
    return number


def query_ollama_models(env: Mapping[str, str]) -> List[Dict[str, Any]]:
    """Query the local Ollama instance for downloaded models.

    Returns a list of model dicts sorted by parameter size (largest first),
    or an empty list if Ollama is unreachable.
    """
    try:
        resp = httpx.get(f"{_ollama_host(env)}/api/tags", timeout=2.0)
        resp.raise_for_status()
        models = resp.json().get("models") or []
        for m in models:
            size_str = (m.get("details") or {}).get("parameter_size", "")
            m["_param_size"] = _parse_param_size(size_str)
        models.sort(key=lambda m: m["_param_size"], reverse=True)
        return models
    except Exception:  # noqa: BLE001
        return []


def resolve_ollama_model(env: Mapping[str, str]) -> str:
    """Return the best locally-available Ollama model, or the static fallback."""
    models = query_ollama_models(env)
    if models:
        return models[0]["name"]
    return get_catalog_default("ollama") or OllamaProvider.default_model
