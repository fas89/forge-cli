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
import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from fluid_build.cli.console import cprint

# Check for optional dependencies
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.prompt import Confirm, Prompt
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
        self.schema = config.get("schema")
        self.role = config.get("role")
        self.authenticator = config.get("authenticator")
        self.password = config.get("password")
        self.private_key_path = config.get("private_key_path")
        self.private_key_passphrase = config.get("private_key_passphrase")
        self.oauth_token = config.get("oauth_token")

    def _resolve_settings(self) -> Dict[str, Any]:
        from fluid_build.providers.snowflake.util.config import resolve_snowflake_settings

        return resolve_snowflake_settings(
            account=self.account,
            user=self.user,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role,
            authenticator=self.authenticator,
            password=self.password,
            private_key_path=self.private_key_path,
            private_key_passphrase=self.private_key_passphrase,
            oauth_token=self.oauth_token,
        )

    @staticmethod
    def _has_connector_auth(settings: Dict[str, Any]) -> bool:
        return any(
            settings.get(key)
            for key in ["password", "private_key_path", "oauth_token", "authenticator"]
        )

    def _check_auth_with_connector(self, settings: Dict[str, Any]) -> AuthResult:
        from fluid_build.providers.snowflake.connection import SnowflakeConnection
        from fluid_build.providers.snowflake.util.config import get_connection_params

        params = get_connection_params(
            account=settings.get("account"),
            warehouse=settings.get("warehouse"),
            database=settings.get("database"),
            schema=settings.get("schema"),
            user=settings.get("user"),
            role=settings.get("role"),
            authenticator=settings.get("authenticator"),
            password=settings.get("password"),
            private_key_path=settings.get("private_key_path"),
            private_key_passphrase=settings.get("private_key_passphrase"),
            oauth_token=settings.get("oauth_token"),
        )

        with SnowflakeConnection(**params) as conn:
            rows = conn.execute(
                "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
            )
            current_user, current_role, current_warehouse, current_database, current_schema = (
                rows[0] if rows else ("unknown", "unknown", "unknown", "unknown", "unknown")
            )

        return AuthResult(
            provider=self.name,
            status=AuthStatus.AUTHENTICATED,
            user_info={
                "account": settings.get("account"),
                "user": current_user or settings.get("user"),
                "warehouse": current_warehouse or settings.get("warehouse"),
                "database": current_database or settings.get("database"),
                "schema": current_schema or settings.get("schema"),
                "role": current_role or settings.get("role"),
                "authenticator": settings.get("authenticator") or "password",
            },
        )

    def _login_with_snowsql(self) -> AuthResult:
        """Fallback interactive SnowSQL login for SSO/browser-first setups."""
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

        error_msg = result.stderr.strip() if result.stderr else "Authentication failed"
        return AuthResult(
            provider=self.name, status=AuthStatus.NOT_AUTHENTICATED, error_message=error_msg
        )

    def _check_auth_with_snowsql(self) -> AuthResult:
        """Fallback status check using SnowSQL when no connector auth is configured."""
        try:
            self._run_command(["snowsql", "--version"], capture_output=True)
        except CLIError:
            return AuthResult(
                provider=self.name,
                status=AuthStatus.ERROR,
                error_message="SnowSQL CLI not installed. Please install SnowSQL from Snowflake.",
            )

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

            return AuthResult(
                provider=self.name,
                status=AuthStatus.NOT_AUTHENTICATED,
                error_message="Snowflake credentials not configured or invalid",
            )

        return AuthResult(
            provider=self.name,
            status=AuthStatus.NOT_AUTHENTICATED,
            error_message="Snowflake account and user not configured",
        )

    async def login(self, **kwargs) -> AuthResult:
        """Validate Snowflake authentication using the same connector path as the provider."""
        try:
            settings = self._resolve_settings()
            self.account = settings.get("account")
            self.user = settings.get("user")
            self.warehouse = settings.get("warehouse")
            self.database = settings.get("database")
            self.schema = settings.get("schema")
            self.role = settings.get("role")
            missing = [key for key in ["account", "user"] if not settings.get(key)]
            if missing and self._has_connector_auth(settings):
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.NOT_AUTHENTICATED,
                    error_message=(
                        "Snowflake connection is missing required settings: "
                        + ", ".join(missing)
                        + ". Set them in the contract binding, credential store, or SNOWFLAKE_* env vars."
                    ),
                )

            if self._has_connector_auth(settings):
                return self._check_auth_with_connector(settings)

            return self._login_with_snowsql()
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
            settings = self._resolve_settings()
            self.account = settings.get("account")
            self.user = settings.get("user")
            self.warehouse = settings.get("warehouse")
            self.database = settings.get("database")
            self.schema = settings.get("schema")
            self.role = settings.get("role")

            if not (settings.get("account") and settings.get("user")) and self._has_connector_auth(
                settings
            ):
                return AuthResult(
                    provider=self.name,
                    status=AuthStatus.NOT_AUTHENTICATED,
                    error_message="Snowflake account and user not configured",
                )

            if self._has_connector_auth(settings):
                return self._check_auth_with_connector(settings)

            return self._check_auth_with_snowsql()
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


class AuthManager:
    """Manages authentication for multiple providers"""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.console = Console() if RICH_AVAILABLE else None
        self.providers: Dict[str, AuthProvider] = {}
        self._initialize_providers()

    def _initialize_providers(self):
        """Initialize available authentication providers"""
        provider_classes = {
            "google_cloud": GoogleCloudAuthProvider,
            "gcp": GoogleCloudAuthProvider,  # Alias
            "google": GoogleCloudAuthProvider,  # Alias
            "aws": AWSAuthProvider,
            "amazon": AWSAuthProvider,  # Alias
            "azure": AzureAuthProvider,
            "microsoft": AzureAuthProvider,  # Alias
            "snowflake": SnowflakeAuthProvider,
            "databricks": DatabricksAuthProvider,
        }

        for provider_name, provider_class in provider_classes.items():
            try:
                # Get provider config from various sources
                provider_config = {}

                # Check for provider-specific config
                if provider_name in self.config:
                    provider_config.update(self.config[provider_name])

                # Check for the base name in config (e.g., 'aws' for 'amazon')
                base_name = {
                    "gcp": "google_cloud",
                    "google": "google_cloud",
                    "amazon": "aws",
                    "microsoft": "azure",
                }.get(provider_name, provider_name)

                if base_name in self.config and base_name != provider_name:
                    provider_config.update(self.config[base_name])

                self.providers[provider_name] = provider_class(provider_config, self.logger)
            except Exception as e:
                self.logger.warning(f"Failed to initialize {provider_name} provider: {e}")

    def get_provider(self, provider_name: str) -> Optional[AuthProvider]:
        """Get authentication provider by name"""
        return self.providers.get(provider_name.lower())

    def list_providers(self) -> List[str]:
        """List available authentication providers"""
        # Return unique provider types (not aliases)
        unique_providers = []
        seen_classes = set()
        for name, provider in self.providers.items():
            provider_class = type(provider).__name__
            if provider_class not in seen_classes:
                unique_providers.append(name)
                seen_classes.add(provider_class)
        return unique_providers

    async def login(self, provider_name: str, **kwargs) -> AuthResult:
        """Login to specified provider"""
        provider = self.get_provider(provider_name)
        if not provider:
            return AuthResult(
                provider=provider_name,
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
        provider = self.get_provider(provider_name)
        if not provider:
            return AuthResult(
                provider=provider_name,
                status=AuthStatus.ERROR,
                error_message=f"Provider '{provider_name}' not supported",
            )

        return await provider.check_auth()


# Enhanced CLI Registration
def register(subparsers: argparse._SubParsersAction):
    """Register the auth command with enhanced functionality"""
    p = subparsers.add_parser(COMMAND, help="Provider authentication management")

    # Add provider argument
    p.add_argument(
        "--provider",
        "-p",
        help="Cloud provider (google_cloud, aws, azure, snowflake, databricks, gcp, amazon, microsoft)",
        choices=[
            "google_cloud",
            "gcp",
            "google",
            "aws",
            "amazon",
            "azure",
            "microsoft",
            "snowflake",
            "databricks",
        ],
    )

    # Create subcommands
    sp = p.add_subparsers(dest="verb", required=True, help="Authentication action")

    # Login command
    login_parser = sp.add_parser("login", help="Authenticate with a cloud provider")
    login_parser.add_argument(
        "provider", nargs="?", help="Provider to authenticate with (overrides --provider)"
    )
    login_parser.set_defaults(func=run)

    # Status command
    status_parser = sp.add_parser("status", help="Show authentication status")
    status_parser.add_argument(
        "provider", nargs="?", help="Provider to check (if not specified, checks all)"
    )
    status_parser.set_defaults(func=run)

    # Logout command
    logout_parser = sp.add_parser("logout", help="Logout from a provider")
    logout_parser.add_argument(
        "provider", nargs="?", help="Provider to logout from (overrides --provider)"
    )
    logout_parser.set_defaults(func=run)

    # List providers command
    list_parser = sp.add_parser("list", help="List available authentication providers")
    list_parser.set_defaults(func=run)

    p.set_defaults(cmd=COMMAND, func=run)


def run(args, logger: logging.Logger) -> int:
    """Main entry point for auth command with enhanced functionality"""
    try:
        # Simple config (since load_config is not available)
        config = {}
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

                provider_info = {
                    "google_cloud": ("gcp, google", "Google Cloud Platform"),
                    "aws": ("amazon", "Amazon Web Services"),
                    "azure": ("microsoft", "Microsoft Azure"),
                    "snowflake": ("", "Snowflake Data Cloud"),
                    "databricks": ("", "Databricks Unified Analytics Platform"),
                }

                for provider in providers:
                    if provider in provider_info:
                        aliases, description = provider_info[provider]
                        table.add_row(provider, aliases, description)

                console.print(table)
                console.print("\n[dim]Usage: fluid --provider <provider> login[/dim]")
            else:
                cprint("Available authentication providers:")
                for provider in providers:
                    cprint(f"  - {provider}")
                cprint("\nUsage: fluid --provider <provider> login")

            return 0

        # Determine provider
        provider = getattr(args, "provider", None)

        # Handle commands that can take provider as positional argument
        if hasattr(args, "provider") and args.provider:
            # Provider specified as positional argument
            provider = args.provider
        elif hasattr(args, "provider") and not args.provider:
            # Check if provider was specified as --provider flag
            provider = getattr(args, "provider", None)

        # Run async commands
        if args.verb == "login":
            if not provider:
                logger.error(
                    "❌ Provider required for login. Use: fluid --provider <provider> login"
                )
                logger.info(f"Available providers: {', '.join(auth_manager.list_providers())}")
                return 1

            return asyncio.run(handle_login(provider, auth_manager, logger))

        elif args.verb == "status":
            return asyncio.run(handle_status(provider, auth_manager, logger))

        elif args.verb == "logout":
            if not provider:
                logger.error(
                    "❌ Provider required for logout. Use: fluid --provider <provider> logout"
                )
                return 1

            return asyncio.run(handle_logout(provider, auth_manager, logger))

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
                    console.print(f"\n[dim]💡 Run: fluid --provider {provider} login[/dim]")
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
                console.print(
                    "\n[dim]💡 Use: fluid --provider <provider> login to authenticate[/dim]"
                )
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
