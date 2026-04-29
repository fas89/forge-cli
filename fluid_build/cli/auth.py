#!/usr/bin/env python3
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
FLUID Authentication CLI
Provides unified authentication for various cloud and data platform providers
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from fluid_build.cli.console import cprint
from fluid_build.config_manager import FluidConfig
from fluid_build.credentials import CatalogCredentialAdapter, CredentialSource
from fluid_build.credentials.adapters import CATALOG_PROVIDER_ALIASES

# Check for optional dependencies
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.prompt import Confirm
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

COMMAND = "auth"


class CLIError(Exception):
    """Custom exception for CLI errors"""

    def __init__(self, code: int, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


class AuthStatus(Enum):
    """Authentication status"""

    AUTHENTICATED = "authenticated"
    NOT_AUTHENTICATED = "not_authenticated"
    EXPIRED = "expired"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class AuthResult:
    """Authentication result information"""

    provider: str
    status: AuthStatus
    user_info: Dict[str, Any] = field(default_factory=dict)
    credentials_path: Optional[str] = None
    expires_at: Optional[str] = None
    scopes: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


CATALOG_AUTH_SPECS: Dict[str, Dict[str, Any]] = {
    "dmm": {
        "catalog_names": ["datamesh-manager", "dmm", "entropy-data"],
        "keys": ["api_key"],
        "preferred_key": "api_key",
        "description": "Entropy Data / Data Mesh Manager",
    },
    "datahub": {
        "catalog_names": ["datahub"],
        "keys": ["token"],
        "preferred_key": "token",
        "description": "DataHub",
    },
    "atlan": {
        "catalog_names": ["atlan"],
        "keys": ["api_key"],
        "preferred_key": "api_key",
        "description": "Atlan",
    },
    "collibra": {
        "catalog_names": ["collibra"],
        "keys": ["token", "username", "password"],
        "preferred_key": "token",
        "description": "Collibra",
    },
}

CLOUD_PROVIDER_ALIASES = {
    "gcp": "google_cloud",
    "google": "google_cloud",
    "amazon": "aws",
    "microsoft": "azure",
}

PROVIDER_INFO: Dict[str, Dict[str, str]] = {
    "google_cloud": {"aliases": "gcp, google", "description": "Google Cloud Platform"},
    "aws": {"aliases": "amazon", "description": "Amazon Web Services"},
    "azure": {"aliases": "microsoft", "description": "Microsoft Azure"},
    "snowflake": {"aliases": "", "description": "Snowflake Data Cloud"},
    "databricks": {"aliases": "", "description": "Databricks Unified Analytics Platform"},
    "dmm": {"aliases": "datamesh-manager, entropy-data", "description": "Entropy Data / Data Mesh Manager"},
    "datahub": {"aliases": "", "description": "DataHub"},
    "atlan": {"aliases": "", "description": "Atlan"},
    "collibra": {"aliases": "", "description": "Collibra"},
}

SUPPORTED_PROVIDER_CHOICES = list(PROVIDER_INFO.keys()) + [
    alias
    for alias in list(CLOUD_PROVIDER_ALIASES.keys()) + list(CATALOG_PROVIDER_ALIASES.keys())
    if alias not in PROVIDER_INFO
]


class AuthProvider:
    """Base class for authentication providers"""

    def __init__(self, name: str, config: Dict[str, Any], logger: logging.Logger):
        self.name = name
        self.config = config
        self.logger = logger
        self.console = Console() if RICH_AVAILABLE else None

    async def login(self, **kwargs) -> AuthResult:
        """Initiate login flow for this provider"""
        raise NotImplementedError("Subclasses must implement login method")

    async def logout(self) -> bool:
        """Logout from this provider"""
        raise NotImplementedError("Subclasses must implement logout method")

    async def check_auth(self) -> AuthResult:
        """Check current authentication status"""
        raise NotImplementedError("Subclasses must implement check_auth method")

    def _run_command(
        self, command: List[str], capture_output: bool = True, check: bool = True
    ) -> subprocess.CompletedProcess:
        """Run a shell command with proper error handling"""
        try:
            self.logger.debug(f"Running command: {' '.join(command)}")
            result = subprocess.run(command, capture_output=capture_output, text=True, check=check)
            return result
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Command failed: {e}")
            raise
        except FileNotFoundError as e:
            self.logger.error(f"Command not found: {command[0]} - {e}")
            raise CLIError(1, "command_not_found", {"command": command[0]})


def _normalize_provider_name(provider_name: Optional[str]) -> Optional[str]:
    """Normalize provider aliases to canonical provider names."""

    if not provider_name:
        return None

    normalized = provider_name.strip().lower()
    normalized = CLOUD_PROVIDER_ALIASES.get(normalized, normalized)
    normalized = CATALOG_PROVIDER_ALIASES.get(normalized, normalized)
    return normalized


def _arg_value(args: Any, name: str) -> Any:
    """Safely read argparse-style attributes from test doubles and namespaces."""
    try:
        values = vars(args)
    except TypeError:
        return None
    return values.get(name)


_SOURCE_LABELS: Dict[CredentialSource, str] = {
    CredentialSource.CLI_ARGUMENT: "cli",
    CredentialSource.ENVIRONMENT: "environment",
    CredentialSource.DOTENV: ".env",
    CredentialSource.KEYRING: "keyring",
    CredentialSource.ENCRYPTED_FILE: "encrypted file",
    CredentialSource.CONFIG_FILE: "config",
    CredentialSource.VAULT: "vault",
    CredentialSource.SECRET_MANAGER: "secret manager",
    CredentialSource.PROVIDER_DEFAULT: "provider default",
    CredentialSource.PROMPT: "prompt",
}


def _source_label(source: Optional[CredentialSource]) -> str:
    """Render a credential source for status output."""
    if source is None:
        return "unknown"
    return _SOURCE_LABELS.get(source, "unknown")


class GoogleCloudAuthProvider(AuthProvider):
    """Google Cloud Platform authentication provider"""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        super().__init__("google_cloud", config, logger)
        self.project_id = config.get("project_id")
        self.scopes = config.get(
            "scopes",
            [
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/datacatalog",
            ],
        )

    async def login(self, **kwargs) -> AuthResult:
        """Initiate Google Cloud authentication flow"""
        try:
            if self.console and RICH_AVAILABLE:
                self.console.print(
                    Panel.fit(
                        "[bold blue]🔐 Google Cloud Authentication[/bold blue]\n\n"
                        "This will open your web browser to complete authentication.\n"
                        f"Project: [cyan]{self.project_id or 'Not specified'}[/cyan]",
                        border_style="blue",
                    )
                )

                if not Confirm.ask("\nProceed with authentication?", default=True):
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="User cancelled authentication",
                    )

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=self.console,
                ) as progress:
                    task = progress.add_task(
                        "Configuring application default credentials...", total=1
                    )

                    # Run gcloud auth application-default login
                    command = ["gcloud", "auth", "application-default", "login"]
                    if self.scopes:
                        command.extend(["--scopes", ",".join(self.scopes)])
                    if self.project_id:
                        command.extend(["--project", self.project_id])

                    self._run_command(command, capture_output=False)
                    progress.update(task, completed=1)
            else:
                cprint("🔐 Initiating Google Cloud authentication...")
                command = ["gcloud", "auth", "application-default", "login"]
                if self.scopes:
                    command.extend(["--scopes", ",".join(self.scopes)])
                if self.project_id:
                    command.extend(["--project", self.project_id])
                self._run_command(command, capture_output=False)

            # Verify authentication
            return await self.check_auth()

        except Exception as e:
            return AuthResult(
                provider=self.name,
                status=AuthStatus.ERROR,
                error_message=f"Google Cloud authentication failed: {e}",
            )

    async def logout(self) -> bool:
        """Logout from Google Cloud"""
        try:
            # Revoke application default credentials
            try:
                self._run_command(["gcloud", "auth", "application-default", "revoke"], check=False)
            except Exception:
                pass

            # Revoke user credentials
            try:
                self._run_command(["gcloud", "auth", "revoke", "--all"], check=False)
            except Exception:
                pass

            # Remove environment variable
            if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

            self.logger.info("Google Cloud logout completed")
            return True

        except Exception as e:
            self.logger.error(f"Google Cloud logout failed: {e}")
            return False

    async def check_auth(self) -> AuthResult:
        """Check Google Cloud authentication status"""
        try:
            # Check if gcloud is installed
            try:
                self._run_command(["gcloud", "version"], capture_output=True)
            except Exception:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.ERROR,
                    error_message="gcloud CLI not installed. Please install Google Cloud SDK.",
                )

            # Check application default credentials
            try:
                result = self._run_command(
                    ["gcloud", "auth", "application-default", "print-access-token"],
                    capture_output=True,
                )
                if result.returncode == 0:
                    # Get user info
                    try:
                        account_result = self._run_command(
                            ["gcloud", "config", "get-value", "account"], capture_output=True
                        )
                        account = (
                            account_result.stdout.strip() if account_result.stdout else "unknown"
                        )

                        project_result = self._run_command(
                            ["gcloud", "config", "get-value", "project"], capture_output=True
                        )
                        project = (
                            project_result.stdout.strip()
                            if project_result.stdout
                            else self.project_id
                        )

                        return AuthResult(
                            provider=self.name,
                            status=AuthStatus.AUTHENTICATED,
                            user_info={
                                "account": account,
                                "project": project,
                                "cli_version": "installed",
                            },
                            scopes=self.scopes,
                        )
                    except Exception:
                        return AuthResult(
                            provider=self.name,
                            status=AuthStatus.AUTHENTICATED,
                            user_info={"account": "authenticated"},
                            scopes=self.scopes,
                        )
                else:
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="No valid application default credentials found",
                    )

            except subprocess.CalledProcessError:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.NOT_AUTHENTICATED,
                    error_message="Application default credentials not configured",
                )

        except Exception as e:
            return AuthResult(provider=self.name, status=AuthStatus.ERROR, error_message=str(e))


class AWSAuthProvider(AuthProvider):
    """Amazon Web Services authentication provider"""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        super().__init__("aws", config, logger)
        self.region = config.get("region", "us-east-1")
        self.profile = config.get("profile", "default")

    async def login(self, **kwargs) -> AuthResult:
        """Initiate AWS authentication flow"""
        try:
            if self.console and RICH_AVAILABLE:
                self.console.print(
                    Panel.fit(
                        "[bold blue]🔐 AWS Authentication[/bold blue]\n\n"
                        f"Region: [cyan]{self.region}[/cyan]\n"
                        f"Profile: [cyan]{self.profile}[/cyan]\n\n"
                        "This will initiate AWS SSO login or configure credentials.",
                        border_style="blue",
                    )
                )

                if not Confirm.ask("\nProceed with AWS authentication?", default=True):
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="User cancelled authentication",
                    )

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=self.console,
                ) as progress:
                    task = progress.add_task("Initiating AWS SSO login...", total=1)

                    # Try SSO login first, fallback to configure
                    try:
                        command = ["aws", "sso", "login", "--profile", self.profile]
                        self._run_command(command, capture_output=False)
                    except Exception:
                        # Fallback to aws configure
                        self.console.print(
                            "\n[yellow]SSO not configured. Setting up AWS credentials...[/yellow]"
                        )
                        command = ["aws", "configure", "--profile", self.profile]
                        self._run_command(command, capture_output=False)

                    progress.update(task, completed=1)
            else:
                cprint("🔐 Initiating AWS authentication...")
                try:
                    command = ["aws", "sso", "login", "--profile", self.profile]
                    self._run_command(command, capture_output=False)
                except Exception:
                    cprint("SSO not configured. Setting up AWS credentials...")
                    command = ["aws", "configure", "--profile", self.profile]
                    self._run_command(command, capture_output=False)

            return await self.check_auth()

        except Exception as e:
            return AuthResult(
                provider=self.name,
                status=AuthStatus.ERROR,
                error_message=f"AWS authentication failed: {e}",
            )

    async def logout(self) -> bool:
        """Logout from AWS"""
        try:
            # For AWS SSO
            try:
                self._run_command(["aws", "sso", "logout", "--profile", self.profile], check=False)
            except Exception:
                pass

            self.logger.info("AWS logout completed")
            return True

        except Exception as e:
            self.logger.error(f"AWS logout failed: {e}")
            return False

    async def check_auth(self) -> AuthResult:
        """Check AWS authentication status"""
        try:
            # Check if AWS CLI is installed
            try:
                self._run_command(["aws", "--version"], capture_output=True)
            except Exception:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.ERROR,
                    error_message="AWS CLI not installed. Please install AWS CLI.",
                )

            # Check credentials
            try:
                command = [
                    "aws",
                    "sts",
                    "get-caller-identity",
                    "--profile",
                    self.profile,
                    "--output",
                    "json",
                ]
                result = self._run_command(command, capture_output=True)

                if result.returncode == 0:
                    identity = json.loads(result.stdout)
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.AUTHENTICATED,
                        user_info={
                            "user_id": identity.get("UserId"),
                            "account": identity.get("Account"),
                            "arn": identity.get("Arn"),
                            "profile": self.profile,
                            "region": self.region,
                        },
                    )
                else:
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="No valid AWS credentials found",
                    )

            except subprocess.CalledProcessError:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.NOT_AUTHENTICATED,
                    error_message="AWS credentials not configured or expired",
                )

        except Exception as e:
            return AuthResult(provider=self.name, status=AuthStatus.ERROR, error_message=str(e))


class AzureAuthProvider(AuthProvider):
    """Microsoft Azure authentication provider"""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        super().__init__("azure", config, logger)
        self.tenant_id = config.get("tenant_id")
        self.subscription_id = config.get("subscription_id")

    async def login(self, **kwargs) -> AuthResult:
        """Initiate Azure authentication flow"""
        try:
            if self.console and RICH_AVAILABLE:
                self.console.print(
                    Panel.fit(
                        "[bold blue]🔐 Azure Authentication[/bold blue]\n\n"
                        f"Tenant: [cyan]{self.tenant_id or 'Default'}[/cyan]\n"
                        f"Subscription: [cyan]{self.subscription_id or 'Default'}[/cyan]\n\n"
                        "This will open your web browser to complete authentication.",
                        border_style="blue",
                    )
                )

                if not Confirm.ask("\nProceed with Azure authentication?", default=True):
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="User cancelled authentication",
                    )

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=self.console,
                ) as progress:
                    task = progress.add_task("Initiating Azure login...", total=1)

                    command = ["az", "login"]
                    if self.tenant_id:
                        command.extend(["--tenant", self.tenant_id])

                    self._run_command(command, capture_output=False)

                    # Set subscription if provided
                    if self.subscription_id:
                        self._run_command(
                            ["az", "account", "set", "--subscription", self.subscription_id]
                        )

                    progress.update(task, completed=1)
            else:
                cprint("🔐 Initiating Azure authentication...")
                command = ["az", "login"]
                if self.tenant_id:
                    command.extend(["--tenant", self.tenant_id])

                self._run_command(command, capture_output=False)

                if self.subscription_id:
                    self._run_command(
                        ["az", "account", "set", "--subscription", self.subscription_id]
                    )

            return await self.check_auth()

        except Exception as e:
            return AuthResult(
                provider=self.name,
                status=AuthStatus.ERROR,
                error_message=f"Azure authentication failed: {e}",
            )

    async def logout(self) -> bool:
        """Logout from Azure"""
        try:
            self._run_command(["az", "logout"], check=False)
            self.logger.info("Azure logout completed")
            return True

        except Exception as e:
            self.logger.error(f"Azure logout failed: {e}")
            return False

    async def check_auth(self) -> AuthResult:
        """Check Azure authentication status"""
        try:
            # Check if Azure CLI is installed
            try:
                self._run_command(["az", "--version"], capture_output=True)
            except Exception:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.ERROR,
                    error_message="Azure CLI not installed. Please install Azure CLI.",
                )

            # Check authentication
            try:
                result = self._run_command(
                    ["az", "account", "show", "--output", "json"], capture_output=True
                )

                if result.returncode == 0:
                    account_info = json.loads(result.stdout)
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.AUTHENTICATED,
                        user_info={
                            "name": account_info.get("name"),
                            "id": account_info.get("id"),
                            "tenant_id": account_info.get("tenantId"),
                            "user": account_info.get("user", {}).get("name"),
                            "type": account_info.get("user", {}).get("type"),
                        },
                    )
                else:
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="No active Azure session found",
                    )

            except subprocess.CalledProcessError:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.NOT_AUTHENTICATED,
                    error_message="Azure CLI not authenticated",
                )

        except Exception as e:
            return AuthResult(provider=self.name, status=AuthStatus.ERROR, error_message=str(e))


class SnowflakeAuthProvider(AuthProvider):
    """Snowflake authentication provider"""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        super().__init__("snowflake", config, logger)
        self.account = config.get("account")
        self.user = config.get("user")
        self.warehouse = config.get("warehouse")
        self.database = config.get("database")
        self.role = config.get("role")

    async def login(self, **kwargs) -> AuthResult:
        """Initiate Snowflake authentication using SnowSQL"""
        try:
            # Check if SnowSQL is installed
            try:
                self._run_command(["snowsql", "--version"], capture_output=True)
            except CLIError:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.ERROR,
                    error_message="SnowSQL CLI not installed. Please install SnowSQL from Snowflake.",
                )

            if self.console:
                self.console.print(
                    Panel(
                        f"🏔️ Snowflake Authentication\n\n"
                        f"Account: [cyan]{self.account or 'Not specified'}[/cyan]\n"
                        f"User: [cyan]{self.user or 'Not specified'}[/cyan]\n"
                        f"Warehouse: [cyan]{self.warehouse or 'Not specified'}[/cyan]\n\n"
                        "This will prompt for your Snowflake credentials.",
                        border_style="blue",
                    )
                )

                if not Confirm.ask("\nProceed with Snowflake authentication?", default=True):
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="User cancelled authentication",
                    )

            # Build connection parameters
            connection_params = []
            if self.account:
                connection_params.extend(["-a", self.account])
            if self.user:
                connection_params.extend(["-u", self.user])
            if self.warehouse:
                connection_params.extend(["-w", self.warehouse])
            if self.database:
                connection_params.extend(["-d", self.database])
            if self.role:
                connection_params.extend(["-r", self.role])

            if self.console:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=self.console,
                ) as progress:
                    task = progress.add_task("Connecting to Snowflake...", total=1)

                    # Test connection with a simple query
                    command = ["snowsql"] + connection_params + ["-q", "SELECT CURRENT_USER();"]
                    result = self._run_command(command, capture_output=True, check=False)

                    progress.update(task, completed=1)
            else:
                command = ["snowsql"] + connection_params + ["-q", "SELECT CURRENT_USER();"]
                result = self._run_command(command, capture_output=True, check=False)

            if result.returncode == 0:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.AUTHENTICATED,
                    user_info={
                        "account": self.account,
                        "user": self.user,
                        "warehouse": self.warehouse,
                        "database": self.database,
                        "role": self.role,
                        "cli_version": "installed",
                    },
                )
            else:
                error_msg = result.stderr.strip() if result.stderr else "Authentication failed"
                return AuthResult(
                    provider=self.name, status=AuthStatus.NOT_AUTHENTICATED, error_message=error_msg
                )

        except Exception as e:
            return AuthResult(provider=self.name, status=AuthStatus.ERROR, error_message=str(e))

    async def logout(self) -> bool:
        """Logout from Snowflake (clear stored credentials)"""
        # Snowflake doesn't maintain persistent sessions like cloud providers
        # But we can clear any stored connection info
        self.logger.info("Snowflake logout completed (session-based authentication)")
        return True

    async def check_auth(self) -> AuthResult:
        """Check Snowflake authentication status"""
        try:
            # Check if SnowSQL is installed
            try:
                self._run_command(["snowsql", "--version"], capture_output=True)
            except CLIError:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.ERROR,
                    error_message="SnowSQL CLI not installed. Please install SnowSQL from Snowflake.",
                )

            # Test connection if we have basic params
            if self.account and self.user:
                connection_params = ["-a", self.account, "-u", self.user]
                if self.warehouse:
                    connection_params.extend(["-w", self.warehouse])
                if self.database:
                    connection_params.extend(["-d", self.database])
                if self.role:
                    connection_params.extend(["-r", self.role])

                command = ["snowsql"] + connection_params + ["-q", "SELECT CURRENT_USER();"]
                result = self._run_command(command, capture_output=True, check=False)

                if result.returncode == 0:
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.AUTHENTICATED,
                        user_info={
                            "account": self.account,
                            "user": self.user,
                            "warehouse": self.warehouse,
                            "database": self.database,
                            "role": self.role,
                        },
                    )
                else:
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="Snowflake credentials not configured or invalid",
                    )
            else:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.NOT_AUTHENTICATED,
                    error_message="Snowflake account and user not configured",
                )

        except Exception as e:
            return AuthResult(provider=self.name, status=AuthStatus.ERROR, error_message=str(e))


class DatabricksAuthProvider(AuthProvider):
    """Databricks authentication provider"""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        super().__init__("databricks", config, logger)
        self.host = config.get("host")
        self.token = config.get("token")
        self.cluster_id = config.get("cluster_id")
        self.workspace_id = config.get("workspace_id")

    async def login(self, **kwargs) -> AuthResult:
        """Initiate Databricks authentication using Databricks CLI"""
        try:
            # Check if Databricks CLI is installed
            try:
                self._run_command(["databricks", "--version"], capture_output=True)
            except CLIError:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.ERROR,
                    error_message="Databricks CLI not installed. Please install: pip install databricks-cli",
                )

            if self.console:
                self.console.print(
                    Panel(
                        f"🧱 Databricks Authentication\n\n"
                        f"Host: [cyan]{self.host or 'Not specified'}[/cyan]\n"
                        f"Workspace ID: [cyan]{self.workspace_id or 'Not specified'}[/cyan]\n"
                        f"Cluster ID: [cyan]{self.cluster_id or 'Not specified'}[/cyan]\n\n"
                        "This will configure Databricks CLI authentication.\n"
                        "You'll need your workspace URL and personal access token.",
                        border_style="blue",
                    )
                )

                if not Confirm.ask("\nProceed with Databricks authentication?", default=True):
                    return AuthResult(
                        provider=self.name,
                        status=AuthStatus.NOT_AUTHENTICATED,
                        error_message="User cancelled authentication",
                    )

            if self.console:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=self.console,
                ) as progress:
                    task = progress.add_task("Configuring Databricks CLI...", total=1)

                    # Configure Databricks CLI
                    command = ["databricks", "configure", "--token"]
                    if self.host:
                        # Non-interactive configuration if host is provided
                        self._run_command(
                            command + ["--host", self.host], capture_output=False, check=False
                        )
                    else:
                        # Interactive configuration
                        self._run_command(command, capture_output=False, check=False)

                    progress.update(task, completed=1)
            else:
                command = ["databricks", "configure", "--token"]
                if self.host:
                    self._run_command(
                        command + ["--host", self.host], capture_output=False, check=False
                    )
                else:
                    self._run_command(command, capture_output=False, check=False)

            # Test the configuration
            test_result = self._run_command(
                ["databricks", "workspace", "list", "/"], capture_output=True, check=False
            )

            if test_result.returncode == 0:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.AUTHENTICATED,
                    user_info={
                        "host": self.host,
                        "workspace_id": self.workspace_id,
                        "cluster_id": self.cluster_id,
                        "cli_version": "installed",
                    },
                )
            else:
                error_msg = (
                    test_result.stderr.strip() if test_result.stderr else "Authentication failed"
                )
                return AuthResult(
                    provider=self.name, status=AuthStatus.NOT_AUTHENTICATED, error_message=error_msg
                )

        except Exception as e:
            return AuthResult(provider=self.name, status=AuthStatus.ERROR, error_message=str(e))

    async def logout(self) -> bool:
        """Logout from Databricks (clear stored configuration)"""
        try:
            # Remove Databricks CLI configuration
            config_file = os.path.expanduser("~/.databrickscfg")
            if os.path.exists(config_file):
                os.remove(config_file)
                self.logger.info("Databricks configuration cleared")
            return True
        except Exception as e:
            self.logger.error(f"Failed to clear Databricks configuration: {e}")
            return False

    async def check_auth(self) -> AuthResult:
        """Check Databricks authentication status"""
        try:
            # Check if Databricks CLI is installed
            try:
                self._run_command(["databricks", "--version"], capture_output=True)
            except CLIError:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.ERROR,
                    error_message="Databricks CLI not installed. Please install: pip install databricks-cli",
                )

            # Test authentication by listing workspace
            result = self._run_command(
                ["databricks", "workspace", "list", "/"], capture_output=True, check=False
            )

            if result.returncode == 0:
                # Try to get current user info
                try:
                    user_result = self._run_command(
                        ["databricks", "current-user", "me"], capture_output=True, check=False
                    )
                    user_info = {}
                    if user_result.returncode == 0:
                        import json

                        user_data = json.loads(user_result.stdout)
                        user_info = {
                            "user_name": user_data.get("userName"),
                            "display_name": user_data.get("displayName"),
                            "email": (
                                user_data.get("emails", [{}])[0].get("value")
                                if user_data.get("emails")
                                else None
                            ),
                        }
                except Exception:
                    user_info = {}

                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.AUTHENTICATED,
                    user_info={
                        **user_info,
                        "host": self.host,
                        "workspace_id": self.workspace_id,
                        "cluster_id": self.cluster_id,
                    },
                )
            else:
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.NOT_AUTHENTICATED,
                    error_message="Databricks CLI not configured or credentials invalid",
                )

        except Exception as e:
            return AuthResult(provider=self.name, status=AuthStatus.ERROR, error_message=str(e))


class CatalogAuthProvider(AuthProvider):
    """Secure secret management and auth probes for catalog providers."""

    def __init__(self, name: str, config: Dict[str, Any], logger: logging.Logger):
        canonical_name = _normalize_provider_name(name) or name
        super().__init__(canonical_name, config, logger)
        self.name = canonical_name
        self.spec = CATALOG_AUTH_SPECS[canonical_name]
        self.display_name = self.spec["description"]
        self.credential_adapter = CatalogCredentialAdapter(canonical_name)

    @property
    def supported_keys(self) -> List[str]:
        return list(self.spec["keys"])

    @property
    def preferred_key(self) -> str:
        return str(self.spec["preferred_key"])

    async def login(self, **kwargs) -> AuthResult:
        if self.name == "collibra":
            message = (
                "Interactive login is not implemented for Collibra. "
                "Use `fluid auth set --provider collibra --key token` "
                "(preferred) or set `username` and `password`."
            )
        else:
            message = (
                f"Interactive login is not implemented for {self.display_name}. "
                f"Use `fluid auth set --provider {self.name} --key {self.preferred_key}`."
            )
        return AuthResult(
            provider=self.name,
            status=AuthStatus.ERROR,
            error_message=message,
        )

    async def logout(self) -> bool:
        self.logger.error(
            "Interactive logout is not implemented for %s. Use `fluid auth clear --provider %s --key <credential>`.",
            self.display_name,
            self.name,
        )
        return False

    def set_secret(self, key: str, value: Optional[str] = None, force: bool = False) -> None:
        normalized_key = self._normalize_key(key)
        existing = self.credential_adapter.get_stored_credential(normalized_key)
        if existing and not force and not self._confirm_overwrite(normalized_key):
            raise CLIError(
                1,
                "credential_not_overwritten",
                {"provider": self.name, "key": normalized_key},
            )

        secret_value = value if value is not None else self._prompt_secret(normalized_key)
        if not secret_value:
            raise CLIError(
                1,
                "credential_value_required",
                {"provider": self.name, "key": normalized_key},
            )

        try:
            self.credential_adapter.store_credential(normalized_key, secret_value)
            self.credential_adapter.clear_cache()
        except ImportError as exc:
            raise CLIError(
                1,
                "keyring_unavailable",
                {
                    "provider": self.name,
                    "key": normalized_key,
                    "error": str(exc),
                },
            ) from exc
        except Exception as exc:
            raise CLIError(
                1,
                "credential_store_failed",
                {
                    "provider": self.name,
                    "key": normalized_key,
                    "error": str(exc),
                },
            ) from exc

    def clear_secret(self, key: str) -> bool:
        normalized_key = self._normalize_key(key)
        existing = self.credential_adapter.get_stored_credential(normalized_key)
        try:
            self.credential_adapter.clear_stored_credential(normalized_key)
            self.credential_adapter.clear_cache()
        except ImportError as exc:
            raise CLIError(
                1,
                "keyring_unavailable",
                {
                    "provider": self.name,
                    "key": normalized_key,
                    "error": str(exc),
                },
            ) from exc
        except Exception as exc:
            raise CLIError(
                1,
                "credential_clear_failed",
                {
                    "provider": self.name,
                    "key": normalized_key,
                    "error": str(exc),
                },
            ) from exc
        return existing is not None

    async def check_auth(self) -> AuthResult:
        self.credential_adapter.clear_cache()
        credentials, sources = self._resolve_credentials()
        user_info = self._credential_status(credentials, sources)

        missing_message, auth_mode = self._validate_required_credentials(credentials)
        if auth_mode:
            user_info["auth_mode"] = auth_mode

        if missing_message:
            return AuthResult(
                provider=self.name,
                status=AuthStatus.NOT_AUTHENTICATED,
                user_info=user_info,
                error_message=missing_message,
            )

        try:
            probe_info = self._probe_catalog()
            user_info.update(probe_info)
            return AuthResult(
                provider=self.name,
                status=AuthStatus.AUTHENTICATED,
                user_info=user_info,
            )
        except Exception as exc:
            status, error_message = self._classify_probe_error(exc)
            if "endpoint" not in user_info:
                endpoint = self._configured_endpoint()
                if endpoint:
                    user_info["endpoint"] = endpoint
            return AuthResult(
                provider=self.name,
                status=status,
                user_info=user_info,
                error_message=error_message,
            )

    def _normalize_key(self, key: str) -> str:
        normalized_key = (key or "").strip().lower()
        if normalized_key not in self.supported_keys:
            supported = ", ".join(self.supported_keys)
            raise CLIError(
                1,
                "unsupported_credential_key",
                {"provider": self.name, "key": normalized_key, "supported_keys": supported},
            )
        return normalized_key

    def _prompt_secret(self, key: str) -> str:
        prompt = f"{self.display_name} {key}: "
        return getpass.getpass(prompt)

    def _confirm_overwrite(self, key: str) -> bool:
        prompt = f"Overwrite stored {self.name} {key}?"
        if self.console and RICH_AVAILABLE:
            return Confirm.ask(prompt, default=False)
        response = input(f"{prompt} [y/N]: ")
        return response.strip().lower() in {"y", "yes"}

    def _resolve_credentials(
        self,
    ) -> Tuple[Dict[str, Optional[str]], Dict[str, Optional[CredentialSource]]]:
        values: Dict[str, Optional[str]] = {}
        sources: Dict[str, Optional[CredentialSource]] = {}
        for key in self.supported_keys:
            value, source = self.credential_adapter.get_credential_with_source(
                key,
                required=False,
            )
            values[key] = value
            sources[key] = source
        return values, sources

    def _credential_status(
        self,
        credentials: Dict[str, Optional[str]],
        sources: Dict[str, Optional[CredentialSource]],
    ) -> Dict[str, Any]:
        info: Dict[str, Any] = {}
        for key in self.supported_keys:
            value = credentials.get(key)
            source = sources.get(key)
            if value:
                info[key] = f"present via {_source_label(source)}"
            else:
                info[key] = "missing"
        endpoint = self._configured_endpoint()
        if endpoint:
            info["endpoint"] = endpoint
        return info

    def _catalog_config(self) -> Dict[str, Any]:
        catalogs = self.config.get("catalogs", {}) if isinstance(self.config, dict) else {}
        merged: Dict[str, Any] = {}
        for catalog_name in self.spec.get("catalog_names", []):
            catalog_config = catalogs.get(catalog_name)
            if isinstance(catalog_config, dict):
                merged.update(catalog_config)
        canonical_config = catalogs.get(self.name)
        if isinstance(canonical_config, dict):
            merged.update(canonical_config)
        return merged

    def _configured_endpoint(self) -> Optional[str]:
        catalog_config = self._catalog_config()
        if self.name == "dmm":
            return catalog_config.get("endpoint") or catalog_config.get("api_url") or os.getenv(
                "DMM_API_URL"
            )
        if self.name == "datahub":
            return (
                catalog_config.get("endpoint")
                or catalog_config.get("server_url")
                or os.getenv("DATAHUB_ENDPOINT")
                or os.getenv("DATAHUB_SERVER_URL")
            )
        if self.name == "atlan":
            return (
                catalog_config.get("endpoint")
                or catalog_config.get("base_url")
                or os.getenv("ATLAN_ENDPOINT")
                or os.getenv("ATLAN_BASE_URL")
            )
        if self.name == "collibra":
            return (
                catalog_config.get("endpoint")
                or catalog_config.get("base_url")
                or os.getenv("COLLIBRA_ENDPOINT")
                or os.getenv("COLLIBRA_BASE_URL")
            )
        return None

    def _validate_required_credentials(
        self,
        credentials: Dict[str, Optional[str]],
    ) -> Tuple[Optional[str], Optional[str]]:
        if self.name == "collibra":
            if credentials.get("token"):
                return None, "bearer"
            if credentials.get("username") and credentials.get("password"):
                return None, "basic"
            return (
                "Collibra credentials are missing. Set a bearer token with "
                "`fluid auth set --provider collibra --key token` or store both "
                "`username` and `password`.",
                "none",
            )

        required_key = self.preferred_key
        if credentials.get(required_key):
            if self.name in {"datahub", "atlan"}:
                return None, "bearer"
            if self.name == "dmm":
                return None, "api_key"
            return None, None

        return (
            f"{self.display_name} credentials are missing. Use "
            f"`fluid auth set --provider {self.name} --key {required_key}`.",
            None,
        )

    def _probe_catalog(self) -> Dict[str, Any]:
        catalog_config = self._catalog_config()

        if self.name == "dmm":
            from fluid_build.providers.datamesh_manager.datamesh_manager import (
                DataMeshManagerProvider,
            )

            provider = DataMeshManagerProvider(api_url=catalog_config.get("endpoint") or catalog_config.get("api_url"))
            provider.list_products()
            return {"endpoint": provider.api_url}

        if self.name == "datahub":
            from fluid_build.providers.datahub import DataHubProvider

            provider = DataHubProvider(
                endpoint=catalog_config.get("endpoint"),
                server_url=catalog_config.get("server_url"),
                environment=catalog_config.get("environment", "PROD"),
                domain_urn_map=catalog_config.get("domain_urn_map"),
                owner_urn_map=catalog_config.get("owner_urn_map"),
            )
            return provider.ping()

        if self.name == "atlan":
            from fluid_build.providers.atlan import AtlanProvider

            provider = AtlanProvider(
                endpoint=catalog_config.get("endpoint"),
                base_url=catalog_config.get("base_url"),
                domain_qualified_name_map=catalog_config.get("domain_qualified_name_map"),
                default_domain_prefix=catalog_config.get("default_domain_prefix"),
                product_qualified_name_prefix=catalog_config.get(
                    "product_qualified_name_prefix",
                    "default/product",
                ),
                owner_map=catalog_config.get("owner_map"),
            )
            return provider.ping()

        if self.name == "collibra":
            from fluid_build.providers.collibra import CollibraProvider

            provider = CollibraProvider(
                endpoint=catalog_config.get("endpoint"),
                base_url=catalog_config.get("base_url"),
                data_product_catalog_domain_map=catalog_config.get(
                    "data_product_catalog_domain_map"
                ),
                default_catalog_domain=catalog_config.get("default_catalog_domain"),
                asset_type_names=catalog_config.get("asset_type_names"),
                relation_type_names=catalog_config.get("relation_type_names"),
            )
            return provider.ping()

        raise CLIError(1, "unsupported_catalog_provider", {"provider": self.name})

    def _classify_probe_error(self, exc: Exception) -> Tuple[AuthStatus, str]:
        message = str(exc)
        lowered = message.lower()

        if (
            "http 401" in lowered
            or "http 403" in lowered
            or "unauthorized" in lowered
            or "forbidden" in lowered
            or "token is required" in lowered
            or "api key is required" in lowered
            or "credentials are missing" in lowered
        ):
            return AuthStatus.NOT_AUTHENTICATED, message

        return AuthStatus.ERROR, message


class AuthManager:
    """Manages authentication for multiple providers"""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.console = Console() if RICH_AVAILABLE else None
        self.providers: Dict[str, AuthProvider] = {}
        self._provider_order: List[str] = []
        self._initialize_providers()

    def _initialize_providers(self):
        """Initialize available authentication providers"""
        provider_classes = {
            "google_cloud": GoogleCloudAuthProvider,
            "aws": AWSAuthProvider,
            "azure": AzureAuthProvider,
            "snowflake": SnowflakeAuthProvider,
            "databricks": DatabricksAuthProvider,
        }

        for provider_name, provider_class in provider_classes.items():
            try:
                provider_config = self._resolve_cloud_provider_config(provider_name)
                self.providers[provider_name] = provider_class(provider_config, self.logger)
                self._provider_order.append(provider_name)
            except Exception as e:
                self.logger.warning(f"Failed to initialize {provider_name} provider: {e}")

        for provider_name in CATALOG_AUTH_SPECS:
            try:
                self.providers[provider_name] = CatalogAuthProvider(provider_name, self.config, self.logger)
                self._provider_order.append(provider_name)
            except Exception as e:
                self.logger.warning(f"Failed to initialize {provider_name} provider: {e}")

    def _resolve_cloud_provider_config(self, provider_name: str) -> Dict[str, Any]:
        """Load cloud provider config from legacy and current config shapes."""

        provider_config: Dict[str, Any] = {}
        providers_section = self.config.get("providers", {}) if isinstance(self.config, dict) else {}

        aliases = [provider_name] + [
            alias for alias, target in CLOUD_PROVIDER_ALIASES.items() if target == provider_name
        ]
        for name in aliases:
            if isinstance(self.config.get(name), dict):
                provider_config.update(self.config[name])
            if isinstance(providers_section.get(name), dict):
                provider_config.update(providers_section[name])
        return provider_config

    def is_catalog_provider(self, provider_name: str) -> bool:
        normalized = _normalize_provider_name(provider_name)
        return bool(normalized and normalized in CATALOG_AUTH_SPECS)

    def get_provider(self, provider_name: str) -> Optional[AuthProvider]:
        """Get authentication provider by name"""
        normalized = _normalize_provider_name(provider_name)
        if not normalized:
            return None
        return self.providers.get(normalized)

    def list_providers(self) -> List[str]:
        """List available authentication providers"""
        return [name for name in self._provider_order if name in self.providers]

    async def login(self, provider_name: str, **kwargs) -> AuthResult:
        """Login to specified provider"""
        normalized = _normalize_provider_name(provider_name) or provider_name
        provider = self.get_provider(provider_name)
        if not provider:
            return AuthResult(
                provider=normalized,
                status=AuthStatus.ERROR,
                error_message=f"Provider '{provider_name}' not supported. Available: {', '.join(self.list_providers())}",
            )

        return await provider.login(**kwargs)

    async def logout(self, provider_name: str) -> bool:
        """Logout from specified provider"""
        provider = self.get_provider(provider_name)
        if not provider:
            self.logger.error(f"Provider '{provider_name}' not found")
            return False

        return await provider.logout()

    async def check_auth(self, provider_name: str) -> AuthResult:
        """Check authentication status for specified provider"""
        normalized = _normalize_provider_name(provider_name) or provider_name
        provider = self.get_provider(provider_name)
        if not provider:
            return AuthResult(
                provider=normalized,
                status=AuthStatus.ERROR,
                error_message=f"Provider '{provider_name}' not supported",
            )

        return await provider.check_auth()

    def set_credential(
        self,
        provider_name: str,
        key: str,
        *,
        value: Optional[str] = None,
        force: bool = False,
    ) -> None:
        provider = self.get_provider(provider_name)
        if not isinstance(provider, CatalogAuthProvider):
            normalized = _normalize_provider_name(provider_name) or provider_name
            raise CLIError(
                1,
                "catalog_auth_not_supported",
                {
                    "provider": normalized,
                    "message": (
                        f"Credential storage is only supported for catalog providers. "
                        f"Use interactive login for {normalized}."
                    ),
                },
            )
        provider.set_secret(key, value=value, force=force)

    def clear_credential(self, provider_name: str, key: str) -> bool:
        provider = self.get_provider(provider_name)
        if not isinstance(provider, CatalogAuthProvider):
            normalized = _normalize_provider_name(provider_name) or provider_name
            raise CLIError(
                1,
                "catalog_auth_not_supported",
                {
                    "provider": normalized,
                    "message": (
                        f"Credential clearing is only supported for catalog providers. "
                        f"Use provider logout for {normalized}."
                    ),
                },
            )
        return provider.clear_secret(key)


# Enhanced CLI Registration
def register(subparsers: argparse._SubParsersAction):
    """Register the auth command with enhanced functionality"""
    p = subparsers.add_parser(COMMAND, help="Provider authentication management")

    # Create subcommands
    sp = p.add_subparsers(dest="verb", required=True, help="Authentication action")

    def add_provider_arguments(parser: argparse.ArgumentParser, help_text: str) -> None:
        parser.add_argument("provider", nargs="?", help=help_text)
        parser.add_argument(
            "--provider",
            "-p",
            dest="provider_flag",
            help=help_text,
            choices=SUPPORTED_PROVIDER_CHOICES,
        )

    # Login command
    login_parser = sp.add_parser("login", help="Authenticate with a cloud provider")
    add_provider_arguments(login_parser, "Provider to authenticate with")
    login_parser.set_defaults(func=run)

    # Status command
    status_parser = sp.add_parser("status", help="Show authentication status")
    add_provider_arguments(status_parser, "Provider to check (if not specified, checks all)")
    status_parser.set_defaults(func=run)

    # Logout command
    logout_parser = sp.add_parser("logout", help="Logout from a provider")
    add_provider_arguments(logout_parser, "Provider to logout from")
    logout_parser.set_defaults(func=run)

    # Set command
    set_parser = sp.add_parser("set", help="Store a catalog credential in secure keyring")
    add_provider_arguments(set_parser, "Catalog provider to store a credential for")
    set_parser.add_argument("--key", required=True, help="Credential key to store")
    set_parser.add_argument("--value", help="Credential value (prompts securely if omitted)")
    set_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing stored credential without prompting",
    )
    set_parser.set_defaults(func=run)

    # Clear command
    clear_parser = sp.add_parser("clear", help="Remove a catalog credential from secure keyring")
    add_provider_arguments(clear_parser, "Catalog provider to clear a credential for")
    clear_parser.add_argument("--key", required=True, help="Credential key to remove")
    clear_parser.set_defaults(func=run)

    # List providers command
    list_parser = sp.add_parser("list", help="List available authentication providers")
    list_parser.set_defaults(func=run)

    p.set_defaults(cmd=COMMAND, func=run)


def run(args, logger: logging.Logger) -> int:
    """Main entry point for auth command with enhanced functionality"""
    try:
        config = FluidConfig().to_dict()
        auth_manager = AuthManager(config, logger)

        # Handle list command
        if args.verb == "list":
            console = Console() if RICH_AVAILABLE else None
            providers = auth_manager.list_providers()

            if console:
                console.print("\n[bold blue]🔐 Available Authentication Providers[/bold blue]")
                console.print("=" * 50)

                table = Table()
                table.add_column("Provider", style="cyan")
                table.add_column("Aliases", style="dim")
                table.add_column("Description")

                for provider in providers:
                    info = PROVIDER_INFO.get(provider, {"aliases": "", "description": ""})
                    table.add_row(provider, info["aliases"], info["description"])

                console.print(table)
                console.print("\n[dim]Usage: fluid auth login --provider <provider>[/dim]")
            else:
                cprint("Available authentication providers:")
                for provider in providers:
                    cprint(f"  - {provider}")
                cprint("\nUsage: fluid auth login --provider <provider>")

            return 0

        provider = _arg_value(args, "provider") or _arg_value(args, "provider_flag")

        # Run async commands
        if args.verb == "login":
            if not provider:
                logger.error("❌ Provider required for login. Use: fluid auth login --provider <provider>")
                logger.info(f"Available providers: {', '.join(auth_manager.list_providers())}")
                return 1

            return asyncio.run(handle_login(provider, auth_manager, logger))

        elif args.verb == "status":
            return asyncio.run(handle_status(provider, auth_manager, logger))

        elif args.verb == "logout":
            if not provider:
                logger.error("❌ Provider required for logout. Use: fluid auth logout --provider <provider>")
                return 1

            return asyncio.run(handle_logout(provider, auth_manager, logger))

        elif args.verb == "set":
            if not provider:
                logger.error("❌ Provider required for set. Use: fluid auth set --provider <provider> --key <credential>")
                return 1
            return handle_set(
                provider,
                auth_manager,
                logger,
                key=_arg_value(args, "key"),
                value=_arg_value(args, "value"),
                force=bool(_arg_value(args, "force")),
            )

        elif args.verb == "clear":
            if not provider:
                logger.error("❌ Provider required for clear. Use: fluid auth clear --provider <provider> --key <credential>")
                return 1
            return handle_clear(
                provider,
                auth_manager,
                logger,
                key=_arg_value(args, "key"),
            )

        else:
            # Simplified authentication for compatibility
            logger.info(
                f"Authentication command not fully implemented for verb: {getattr(args, 'verb', 'unknown')}"
            )
            return 0

    except KeyboardInterrupt:
        logger.warning("⚠️ Authentication interrupted by user")
        return 130
    except CLIError:
        raise
    except Exception as e:
        logger.error(f"💥 Authentication failed: {e}")
        raise CLIError(1, "auth_failed", {"error": str(e)})


# Enhanced Handler Functions
async def handle_login(provider: str, auth_manager: AuthManager, logger: logging.Logger) -> int:
    """Handle login command with rich output"""
    try:
        console = Console() if RICH_AVAILABLE else None

        if console:
            console.print("\n[bold green]🔐 FLUID Authentication[/bold green]")
            console.print("=" * 50)

        result = await auth_manager.login(provider)

        if result.status == AuthStatus.AUTHENTICATED:
            if console:
                console.print(
                    f"\n[bold green]✅ Successfully authenticated with {provider}![/bold green]"
                )
                if result.user_info:
                    table = Table(title="Authentication Details", border_style="green")
                    table.add_column("Property", style="cyan")
                    table.add_column("Value", style="green")

                    for key, value in result.user_info.items():
                        table.add_row(key.replace("_", " ").title(), str(value))

                    console.print(table)

                console.print(
                    f"\n[dim]💡 You can now use FLUID to manage resources in {provider}[/dim]"
                )
            else:
                logger.info(f"✅ Successfully authenticated with {provider}")
                if result.user_info:
                    for key, value in result.user_info.items():
                        logger.info(f"{key}: {value}")

            return 0
        else:
            error_msg = result.error_message or "Authentication failed"
            if console:
                console.print(f"\n[bold red]❌ Authentication failed: {error_msg}[/bold red]")

                if "not installed" in error_msg.lower():
                    console.print(
                        Panel.fit(
                            f"[yellow]Please install the required CLI tool for {provider}:\n\n"
                            f"• Google Cloud: https://cloud.google.com/sdk/docs/install\n"
                            f"• AWS: https://aws.amazon.com/cli/\n"
                            f"• Azure: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli[/yellow]",
                            title="Installation Required",
                            border_style="yellow",
                        )
                    )
            else:
                logger.error(f"❌ Authentication failed: {error_msg}")
            return 1

    except Exception as e:
        logger.error(f"❌ Login failed: {e}")
        return 1


async def handle_logout(provider: str, auth_manager: AuthManager, logger: logging.Logger) -> int:
    """Handle logout command"""
    try:
        normalized = _normalize_provider_name(provider) or provider
        if normalized in CATALOG_AUTH_SPECS and auth_manager.is_catalog_provider(normalized):
            logger.error(
                "❌ Interactive logout is not implemented for %s. Use: fluid auth clear --provider %s --key <credential>",
                normalized,
                normalized,
            )
            return 1

        success = await auth_manager.logout(provider)

        if success:
            logger.info(f"✅ Successfully logged out from {provider}")
            return 0
        else:
            logger.error(f"❌ Failed to logout from {provider}")
            return 1

    except Exception as e:
        logger.error(f"❌ Logout failed: {e}")
        return 1


def handle_set(
    provider: str,
    auth_manager: AuthManager,
    logger: logging.Logger,
    *,
    key: Optional[str],
    value: Optional[str],
    force: bool,
) -> int:
    """Handle secure catalog credential storage."""

    try:
        normalized = _normalize_provider_name(provider) or provider
        if normalized not in CATALOG_AUTH_SPECS or not auth_manager.is_catalog_provider(normalized):
            logger.error(
                "❌ `%s` does not use catalog credential storage. Use its native login flow instead.",
                normalized,
            )
            return 1

        auth_manager.set_credential(normalized, key or "", value=value, force=force)
        logger.info(
            "✅ Stored %s credential `%s`. Verify with: fluid auth status --provider %s",
            normalized,
            key,
            normalized,
        )
        return 0
    except CLIError as exc:
        error = exc.details.get("message") or exc.details.get("error") or exc.message
        logger.error(f"❌ Failed to store credential: {error}")
        return exc.code
    except Exception as exc:
        logger.error(f"❌ Failed to store credential: {exc}")
        return 1


def handle_clear(
    provider: str,
    auth_manager: AuthManager,
    logger: logging.Logger,
    *,
    key: Optional[str],
) -> int:
    """Handle secure catalog credential removal."""

    try:
        normalized = _normalize_provider_name(provider) or provider
        if normalized not in CATALOG_AUTH_SPECS or not auth_manager.is_catalog_provider(normalized):
            logger.error(
                "❌ `%s` does not use catalog credential storage. Use its native logout flow instead.",
                normalized,
            )
            return 1

        removed = auth_manager.clear_credential(normalized, key or "")
        if removed:
            logger.info("✅ Cleared %s credential `%s`.", normalized, key)
        else:
            logger.info("ℹ️ No stored %s credential `%s` was found.", normalized, key)
        return 0
    except CLIError as exc:
        error = exc.details.get("message") or exc.details.get("error") or exc.message
        logger.error(f"❌ Failed to clear credential: {error}")
        return exc.code
    except Exception as exc:
        logger.error(f"❌ Failed to clear credential: {exc}")
        return 1


async def handle_status(
    provider: Optional[str], auth_manager: AuthManager, logger: logging.Logger
) -> int:
    """Handle status command with rich output"""
    try:
        console = Console() if RICH_AVAILABLE else None

        if provider:
            # Check specific provider
            result = await auth_manager.check_auth(provider)

            if console:
                status_color = {
                    AuthStatus.AUTHENTICATED: "green",
                    AuthStatus.NOT_AUTHENTICATED: "red",
                    AuthStatus.EXPIRED: "yellow",
                    AuthStatus.ERROR: "red",
                }.get(result.status, "white")

                console.print(
                    f"\n[bold blue]🔍 Authentication Status - {provider.title()}[/bold blue]"
                )
                console.print("=" * 40)
                console.print(
                    f"Status: [{status_color}]{result.status.value.replace('_', ' ').title()}[/{status_color}]"
                )

                if result.user_info:
                    table = Table(title="Account Information", border_style=status_color)
                    table.add_column("Property", style="cyan")
                    table.add_column("Value")

                    for key, value in result.user_info.items():
                        table.add_row(key.replace("_", " ").title(), str(value))

                    console.print(table)

                if result.error_message:
                    console.print(f"\n[red]Error: {result.error_message}[/red]")

                if result.status == AuthStatus.NOT_AUTHENTICATED:
                    normalized = _normalize_provider_name(provider) or provider
                    if normalized in CATALOG_AUTH_SPECS and auth_manager.is_catalog_provider(normalized):
                        preferred_key = CATALOG_AUTH_SPECS[normalized]["preferred_key"]
                        console.print(
                            f"\n[dim]💡 Run: fluid auth set --provider {normalized} --key {preferred_key}[/dim]"
                        )
                    else:
                        console.print(
                            f"\n[dim]💡 Run: fluid auth login --provider {normalized}[/dim]"
                        )
            else:
                cprint(f"{provider}: {result.status.value}")
                if result.user_info:
                    for key, value in result.user_info.items():
                        cprint(f"  {key}: {value}")
                if result.error_message:
                    cprint(f"  Error: {result.error_message}")

            return 0 if result.status == AuthStatus.AUTHENTICATED else 1
        else:
            # Check all providers
            providers = auth_manager.list_providers()
            all_authenticated = True

            if console:
                console.print("\n[bold blue]🔍 Authentication Status - All Providers[/bold blue]")
                console.print("=" * 50)

                table = Table()
                table.add_column("Provider", style="cyan")
                table.add_column("Status", style="bold")
                table.add_column("Account/Details")

                for provider_name in providers:
                    result = await auth_manager.check_auth(provider_name)

                    status_style = {
                        AuthStatus.AUTHENTICATED: "green",
                        AuthStatus.NOT_AUTHENTICATED: "red",
                        AuthStatus.EXPIRED: "yellow",
                        AuthStatus.ERROR: "red",
                    }.get(result.status, "white")

                    if result.status != AuthStatus.AUTHENTICATED:
                        all_authenticated = False

                    details = ""
                    if result.user_info:
                        # Show most relevant detail
                        if "account" in result.user_info:
                            details = result.user_info["account"]
                        elif "user" in result.user_info:
                            details = result.user_info["user"]
                        elif "name" in result.user_info:
                            details = result.user_info["name"]
                    elif result.error_message:
                        details = (
                            result.error_message[:40] + "..."
                            if len(result.error_message) > 40
                            else result.error_message
                        )

                    table.add_row(
                        provider_name.title(),
                        f"[{status_style}]{result.status.value.replace('_', ' ').title()}[/{status_style}]",
                        details,
                    )

                console.print(table)
                console.print("\n[dim]💡 Use `fluid auth status --provider <provider>` for details[/dim]")
            else:
                cprint("Authentication Status:")
                for provider_name in providers:
                    result = await auth_manager.check_auth(provider_name)
                    cprint(f"  {provider_name}: {result.status.value}")
                    if result.status != AuthStatus.AUTHENTICATED:
                        all_authenticated = False
                    if result.error_message:
                        cprint(f"    Error: {result.error_message}")

            return 0 if all_authenticated else 1

    except Exception as e:
        logger.error(f"❌ Status check failed: {e}")
        return 1
