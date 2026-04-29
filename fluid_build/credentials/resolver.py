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

"""
Base credential resolver with multi-source fallback support.
"""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from fluid_build.cli.console import success

logger = logging.getLogger(__name__)


class CredentialSource(Enum):
    """Credential source priority order."""

    CLI_ARGUMENT = 1  # Highest priority
    ENVIRONMENT = 2  # Environment variables
    DOTENV = 3  # .env files
    KEYRING = 4  # OS keyring
    ENCRYPTED_FILE = 5  # Encrypted local file
    CONFIG_FILE = 6  # Config file
    VAULT = 7  # HashiCorp Vault
    SECRET_MANAGER = 8  # Cloud secret managers
    PROVIDER_DEFAULT = 9  # Provider-specific defaults (e.g., ADC for GCP)
    PROMPT = 10  # Interactive prompt (lowest priority)


@dataclass
class CredentialConfig:
    """Configuration for credential resolution."""

    allow_prompt: bool = False
    cache_duration_seconds: int = 3600
    required_sources: Optional[List[CredentialSource]] = None
    project_root: Optional[str] = None
    environment: str = "dev"  # dev, staging, prod


class CredentialError(Exception):
    """Raised when required credential cannot be found."""

    def __init__(self, message: str, suggestions: List[str] = None):
        super().__init__(message)
        self.suggestions = suggestions or []


class BaseCredentialResolver(ABC):
    """
    Base credential resolver with common resolution logic.

    Implements a priority-based credential resolution chain that tries
    multiple sources in order until a credential is found.
    """

    def __init__(self, provider: str, config: Optional[CredentialConfig] = None):
        """
        Initialize credential resolver.

        Args:
            provider: Provider name (e.g., "snowflake", "gcp", "aws")
            config: Optional configuration
        """
        self.provider = provider
        self.config = config or CredentialConfig()
        self._cache: Dict[str, Any] = {}
        self._cache_sources: Dict[str, CredentialSource] = {}

        logger.debug(f"Initialized {provider} credential resolver")

    def get_credential(
        self, key: str, required: bool = True, cli_value: Optional[str] = None, **kwargs
    ) -> Optional[str]:
        value, _ = self.get_credential_with_source(
            key,
            required=required,
            cli_value=cli_value,
            **kwargs,
        )
        return value

    def get_credential_with_source(
        self, key: str, required: bool = True, cli_value: Optional[str] = None, **kwargs
    ) -> Tuple[Optional[str], Optional[CredentialSource]]:
        """
        Resolve credential using priority chain and return the winning source.

        Resolution order:
        1. CLI argument (explicit override)
        2. Environment variable (current session)
        3. .env file (project-specific)
        4. OS Keyring (secure local storage)
        5. Encrypted file (~/.fluid/credentials.enc)
        6. Config file (~/.fluidrc.yaml)
        7. Vault (HashiCorp Vault)
        8. Secret Manager (GCP/AWS/Azure)
        9. Provider-specific default (e.g., ADC for GCP)
        10. Interactive prompt (if allowed)

        Args:
            key: Credential key (e.g., "password", "account")
            required: Whether credential is required
            cli_value: Value from CLI argument (highest priority)
            **kwargs: Additional provider-specific parameters

        Returns:
            Credential value or None if not required and not found

        Raises:
            CredentialError: If required credential not found
        """
        cache_key = f"{self.provider}.{key}"
        if cache_key in self._cache:
            logger.debug(f"Credential '{key}' retrieved from cache")
            return self._cache[cache_key], self._cache_sources.get(cache_key)

        value = None
        source = None

        if cli_value is not None:
            value = cli_value
            source = CredentialSource.CLI_ARGUMENT
            logger.debug(f"Credential '{key}' from CLI argument")

        if value is None:
            value = self._get_from_env(key)
            if value:
                source = CredentialSource.ENVIRONMENT
                logger.debug(f"Credential '{key}' from environment variable")

        if value is None:
            value = self._get_from_dotenv(key)
            if value:
                source = CredentialSource.DOTENV
                logger.debug(f"Credential '{key}' from .env file")

        if value is None:
            value = self._get_from_keyring(key)
            if value:
                source = CredentialSource.KEYRING
                logger.debug(f"Credential '{key}' from OS keyring")

        if value is None:
            value = self._get_from_encrypted_file(key)
            if value:
                source = CredentialSource.ENCRYPTED_FILE
                logger.debug(f"Credential '{key}' from encrypted file")

        if value is None:
            value = self._get_from_config(key)
            if value:
                source = CredentialSource.CONFIG_FILE
                logger.debug(f"Credential '{key}' from config file")

        if value is None:
            value = self._get_from_vault(key)
            if value:
                source = CredentialSource.VAULT
                logger.debug(f"Credential '{key}' from Vault")

        if value is None:
            value = self._get_from_secret_manager(key)
            if value:
                source = CredentialSource.SECRET_MANAGER
                logger.debug(f"Credential '{key}' from secret manager")

        if value is None:
            value = self._get_provider_default(key, **kwargs)
            if value:
                source = CredentialSource.PROVIDER_DEFAULT
                logger.debug(f"Credential '{key}' from provider default")

        if value is None and self.config.allow_prompt and required:
            value = self._get_from_prompt(key)
            if value:
                source = CredentialSource.PROMPT
                logger.debug(f"Credential '{key}' from interactive prompt")

        if value is None and required:
            suggestions = self._get_suggestions(key)
            raise CredentialError(
                f"Required credential not found: {self.provider}.{key}", suggestions=suggestions
            )

        if value is not None:
            self._cache[cache_key] = value
            if source is not None:
                self._cache_sources[cache_key] = source

        return value, source

    def _get_from_env(self, key: str) -> Optional[str]:
        """Get credential from environment variable."""
        env_keys = [
            f"{self.provider.upper()}_{key.upper()}",
            f"{self.provider.upper()}__{key.upper()}",
            key.upper(),
        ]

        for env_key in env_keys:
            value = os.environ.get(env_key)
            if value:
                return value

        return None

    def _get_from_dotenv(self, key: str) -> Optional[str]:
        """Get credential from .env file."""
        try:
            from .dotenv_store import DotEnvCredentialStore

            store = DotEnvCredentialStore(
                project_root=self.config.project_root, environment=self.config.environment
            )

            # Try provider-prefixed key first, then fallback to plain key
            env_keys = [f"{self.provider.upper()}_{key.upper()}", key.upper()]

            for env_key in env_keys:
                value = store.get_credential(env_key)
                if value:
                    return value

            return None

        except ImportError:
            logger.debug("python-dotenv not available, skipping .env file")
            return None
        except Exception as e:
            logger.debug(f"Failed to read from .env file: {e}")
            return None

    def _get_from_keyring(self, key: str) -> Optional[str]:
        """Get credential from OS keyring."""
        try:
            from .keyring_store import KeyringCredentialStore

            keyring_key = f"{self.provider}.{key}"
            return KeyringCredentialStore.get_credential(keyring_key)

        except ImportError:
            logger.debug("keyring library not available, skipping OS keyring")
            return None
        except Exception as e:
            logger.debug(f"Failed to read from keyring: {e}")
            return None

    def _get_from_encrypted_file(self, key: str) -> Optional[str]:
        """Get credential from encrypted file."""
        try:
            from .encrypted_store import EncryptedCredentialStore

            store = EncryptedCredentialStore()
            keyring_key = f"{self.provider}.{key}"
            return store.get_credential(keyring_key)

        except ImportError:
            logger.debug("cryptography library not available, skipping encrypted file")
            return None
        except Exception as e:
            logger.debug(f"Failed to read from encrypted file: {e}")
            return None

    def _get_from_config(self, key: str) -> Optional[str]:
        """Get credential from config file."""
        # TODO: Implement config file support
        return None

    def _get_from_vault(self, key: str) -> Optional[str]:
        """Get credential from HashiCorp Vault."""
        try:
            from ..secrets import get_secret

            secret_name = f"{self.provider}/{key}"
            return get_secret(secret_name, required=False)

        except Exception as e:
            logger.debug(f"Failed to read from Vault: {e}")
            return None

    def _get_from_secret_manager(self, key: str) -> Optional[str]:
        """Get credential from cloud secret manager."""
        # Handled by secrets.py which supports GCP/AWS/Azure
        return None

    def _get_from_prompt(self, key: str) -> Optional[str]:
        """Get credential from interactive prompt."""
        try:
            from getpass import getpass

            prompt = f"Enter {self.provider} {key}: "
            if "password" in key.lower() or "secret" in key.lower() or "token" in key.lower():
                value = getpass(prompt)
            else:
                value = input(prompt)

            # Ask if user wants to save
            if value and self._confirm_save(key):
                self._save_to_keyring(key, value)

            return value

        except Exception as e:
            logger.debug(f"Failed to prompt for credential: {e}")
            return None

    def _confirm_save(self, key: str) -> bool:
        """Ask user if they want to save credential to keyring."""
        try:
            response = input(
                f"Save {self.provider} {key} to secure keyring for future use? (y/n): "
            )
            return response.lower() in ("y", "yes")
        except Exception:
            return False

    def _save_to_keyring(self, key: str, value: str):
        """Save credential to OS keyring."""
        try:
            self.store_credential(key, value)
        except Exception as e:
            logger.warning(f"Failed to save to keyring: {e}")

    def store_credential(self, key: str, value: str) -> None:
        """Store a credential in the OS keyring."""
        from .keyring_store import KeyringCredentialStore

        keyring_key = f"{self.provider}.{key}"
        KeyringCredentialStore.set_credential(keyring_key, value)
        self._cache[keyring_key] = value
        self._cache_sources[keyring_key] = CredentialSource.KEYRING
        success(f"Saved {self.provider} {key} to secure keyring")

    def get_stored_credential(self, key: str) -> Optional[str]:
        """Read a credential directly from the OS keyring."""
        from .keyring_store import KeyringCredentialStore

        keyring_key = f"{self.provider}.{key}"
        return KeyringCredentialStore.get_credential(keyring_key)

    def clear_stored_credential(self, key: str) -> None:
        """Delete a credential from the OS keyring."""
        from .keyring_store import KeyringCredentialStore

        keyring_key = f"{self.provider}.{key}"
        KeyringCredentialStore.delete_credential(keyring_key)
        self._cache.pop(keyring_key, None)
        self._cache_sources.pop(keyring_key, None)

    @abstractmethod
    def _get_provider_default(self, key: str, **kwargs) -> Optional[str]:
        """
        Provider-specific credential retrieval.

        Override this in subclasses to implement provider-specific
        credential resolution (e.g., ADC for GCP, IAM roles for AWS).
        """
        pass

    def _get_suggestions(self, key: str) -> List[str]:
        """Get suggestions for finding credential."""
        return [
            f"Set environment variable: {self.provider.upper()}_{key.upper()}",
            f"Store in keyring: fluid auth set --provider {self.provider} --key {key}",
            f"Create .env file with: {self.provider.upper()}_{key.upper()}=your_value",
            f"Use CLI argument: --{key.replace('_', '-')}",
        ]

    def clear_cache(self):
        """Clear credential cache."""
        self._cache.clear()
        self._cache_sources.clear()
        logger.debug(f"Cleared credential cache for {self.provider}")
